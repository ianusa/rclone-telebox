// Package linkbox provides an interface to the linkbox.to Cloud storage system.
package linkbox

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/juicedata/huaweicloud-sdk-go-obs/obs"
	"github.com/rclone/rclone/backend/linkbox/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	maxEntitiesPerPage        = 64
	minSleep                  = 50 * time.Millisecond // Server sometimes reflects changes slowly
	maxSleep                  = 4 * time.Second
	decayConstant             = 2
	maxPartSize               = 5 * 1_024 * 1_024 * 1_024 // Object Store Service - SDK developer guild
	minPartSize               = 100 * 1024 // Object Store Service - SDK developer guild
	maxPerUploadParts         = 10_000
	defaultPerUploadParts     = 10
)

func init() {
	fsi := &fs.RegInfo{
		Name:           "linkbox",
		Description:    "Linkbox",
		NewFs:          NewFs,
		Options: []fs.Option{{
			Name:       "token",
			Help:       "Token from https://www.linkbox.to/admin/account",
			Required:  true,
		}, {
			Name:       "email",
			Help:       "The email for https://www.linkbox.to/api/user/login_email?email={email}",
			Sensitive: true,
		}, {
			Name:       "password",
			Help:       "The password for https://www.linkbox.to/api/user/login_email?pwd={password}",
			IsPassword: true,
		}, {
			Name:       config.ConfigEncoding,
			Help:       config.ConfigEncodingHelp,
			Advanced:   true,
			Default:    encoder.EncodeInvalidUtf8,
		}, {
			Name:       "multipart_per_upload_parts",
			Help:       "The target number of parts for each multipart upload. 0 to disable.",
			Default:    defaultPerUploadParts,
			Advanced:   true,
		}},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Token string                 `config:"token"`
	Email string                 `config:"email"`
	Password string              `config:"password"`
	Enc encoder.MultiEncoder     `config:"encoding"`
	MultipartPerUploadParts int  `config:"multipart_per_upload_parts"`
}

// Fs stores the interface to the remote Linkbox files
type Fs struct {
	name       string
	root       string
	opt        Options        // options for this backend
	features   *fs.Features   // optional features
	ci         *fs.ConfigInfo // global config
	httpClient *http.Client   // shared http client internally
	srv        *rest.Client   // the connection to the server
	pacer      *fs.Pacer      // pacer for API calls
	accToken   string         // account token
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs          *Fs
	remote      string
	size        int64
	modTime     time.Time
	contentType string
	subType     string
	fullURL     string
	pid         int
	isDir       bool
	id          int
	itemId      string
}

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	ci := fs.GetConfig(ctx)

	httpClient_ := fshttp.NewClient(ctx)
	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		ci:   ci,
		httpClient: httpClient_,
		srv:   rest.NewClient(httpClient_),
		pacer: fs.NewPacer(
			ctx, pacer.NewDefault(pacer.MinSleep(minSleep),
			pacer.MaxSleep(maxSleep),
			pacer.DecayConstant(decayConstant))),
		accToken: "",
	}

	f._UpdateAccountToken(ctx)

	// Account token is the prerequisite for OBS multipart uploads
	// If it is not available, fall back to the default API upload mode
	if f.accToken == "" {
		f.opt.MultipartPerUploadParts = 0
	}

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		WriteMetadata:           false,
		ReadMimeType:            true,
		WriteMimeType:           false,
	}).Fill(ctx, f)

	rootFsObj, err := f._NewObject(ctx, "", true)
	if err == nil {
		rootObj := rootFsObj.(*Object)
		if !rootObj.isDir {
			f.root = path.Dir(f.root)
			return f, fs.ErrorIsFile
		}
	}

	return f, nil
}

func getIdByDirWithRetries(f *Fs, ctx context.Context, dir string) (pid int, err error) {
	f.pacer.Call(func() (bool, error) {
		pid, err = getIdByDir(f, ctx, dir)
		return err != nil, fmt.Errorf("failed to get dirId(%s), err: %w", dir, err)
	})

    return
}

func getIdByDir(f *Fs, ctx context.Context, dir string) (int, error) {
	if dir == "" || dir == "/" || dir =="." {
		return 0, nil // we assume that it is root directory
	}

	path := strings.TrimPrefix(dir, "/")
	dirs := strings.Split(path, "/")
	pid := 0

	for _, tdir := range dirs {
		pageNumber := 0
		numberOfEntities := maxEntitiesPerPage
		isFound := false

		for numberOfEntities == maxEntitiesPerPage {
			oldPid := pid
			pageNumber++
			request := makeSearchQuery("", pid, f.opt.Token, pageNumber)
			responseResult := api.FileSearchRes{}
			err := f._GetUnmarshaledResponse(ctx, request, &responseResult)
			numberOfEntities = len(responseResult.SearchData.Entities)

			if err != nil {
				return 0, err
			}
			if len(responseResult.SearchData.Entities) == 0 {
				return 0, fs.ErrorDirNotFound
			}

			for _, entity := range responseResult.SearchData.Entities {
				if entity.Pid == pid &&
					(entity.Type == "dir" || entity.Type == "sdir") &&
					f.opt.Enc.ToStandardName(entity.Name) == tdir {
					pid = entity.ID
					isFound = true
				}
			}

			// This means that we find directory
			if oldPid != pid {
				break
			}

			if pageNumber > 100000 {
				return 0, fmt.Errorf("too many results")
			}

		}

		if !isFound {
			return 0, fs.ErrorDirNotFound
		}
	}

	return pid, nil
}

func (f *Fs) _FetchDataWithRetries(ctx context.Context, opts *rest.Opts) (resp *http.Response, err error) {
	f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.Call(ctx, opts)
		return err != nil, fmt.Errorf("failed to fetch data, err: %w", err)
	})

    return
}

func (f *Fs) _GetUnmarshaledResponse(ctx context.Context, opts *rest.Opts, result interface{}) error {
	res, err := f._FetchDataWithRetries(ctx, opts)
	if err != nil {
		return err
	}

	responseInByte, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(responseInByte, &result)
	if err != nil {
		return err
	}
	return nil
}

func makeSearchQuery(name string, pid int, token string, pageNubmer int) *rest.Opts {
	return &rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/file_search",
		Parameters: url.Values{
			"name": []string{name},
			"pid":  []string{strconv.Itoa(pid)},
			"token": []string{token},
			"pageNo": []string{strconv.Itoa(pageNubmer)},
			"pageSize": []string{strconv.Itoa(maxEntitiesPerPage)},
		},
	}
}

func (f *Fs) _UpdateAccountToken(ctx context.Context) {
	if (f.opt.Email == "") || (f.opt.Password == "") {
		return
	}

	pass, err := obscure.Reveal(f.opt.Password)
	if err != nil {
		return
	}

	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/user/login_email",
		Parameters: url.Values{
			"email": []string{f.opt.Email},
			"pwd":   []string{pass},
		},
	}

	response := api.LoginRes{}
	err = f._GetUnmarshaledResponse(ctx, &opts, &response)
	if err != nil || response.Status != 1 {
		return
	}

	f.accToken = response.Data.Token
}

func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	if (f.opt.Email == "") || (f.opt.Password == "") {
		return nil, fmt.Errorf("email and password are required")
	}

	pass, err := obscure.Reveal(f.opt.Password)
	if err != nil {
		return nil, fmt.Errorf("error decoding password, obscure it?: %w", err)
	}

	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/user/login_email",
		Parameters: url.Values{
			"email": []string{f.opt.Email},
			"pwd":   []string{pass},
		},
	}

	response := api.LoginRes{}
	err = f._GetUnmarshaledResponse(ctx, &opts, &response)
	if err != nil || response.Status != 1 {
		return nil, fmt.Errorf("error login: %w", err)
	}

	total := response.Data.UserInfo.SizeCap
	used := int64(response.Data.UserInfo.SizeCurr)
	if used < 0 {
		used = 0
	}
	free := total - used

	usage = &fs.Usage{
		Total: fs.NewUsageValue(total),
		Used:  fs.NewUsageValue(used),
		Free: fs.NewUsageValue(free),
	}

	return usage, nil
}

func parse(f *Fs, ctx context.Context, dir string) ([]*Object, error) {
	var responseResult api.FileSearchRes
	var files []*Object
	var numberOfEntities int

	fullPath := path.Join(f.root, dir)
	fullPath = strings.TrimPrefix(fullPath, "/")

	pid, err := getIdByDir(f, ctx, fullPath)

	if err != nil {
		return nil, err
	}

	pageNumber := 0
	numberOfEntities = maxEntitiesPerPage

	for numberOfEntities == maxEntitiesPerPage {
		pageNumber++
		request := makeSearchQuery("", pid, f.opt.Token, pageNumber)

		responseResult = api.FileSearchRes{}
		err = f._GetUnmarshaledResponse(ctx, request, &responseResult)
		if err != nil {
			return nil, fmt.Errorf("parsing failed: %w", err)
		}

		if responseResult.Status != 1 {
			return nil, fmt.Errorf("parsing failed: %s", responseResult.Message)
		}

		numberOfEntities = len(responseResult.SearchData.Entities)

		for _, entity := range responseResult.SearchData.Entities {
			file := &Object{
				fs:          f,
				remote:      f.opt.Enc.ToStandardName(entity.Name),
				modTime:     time.Unix(entity.Ctime, 0),
				contentType: entity.Type,
				subType:     entity.SubType,
				size:        int64(entity.Size),
				fullURL:     entity.URL,
				isDir:       entity.Type == "dir" || entity.Type == "sdir",
				id:          entity.ID,
				itemId:      entity.ItemID,
				pid:         entity.Pid,
			}

			files = append(files, file)
		}

		if pageNumber > 100000 {
			return files, fmt.Errorf("too many results")
		}

	}

	return files, nil
}

func splitDirAndName(remote string) (dir string, name string) {
	lastSlashPosition := strings.LastIndex(remote, "/")
	if lastSlashPosition == -1 {
		dir = ""
		name = remote
	} else {
		dir = remote[:lastSlashPosition]
		name = remote[lastSlashPosition+1:]
	}

	return
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	objects, err := parse(f, ctx, dir)
	if err != nil {
		return nil, err
	}

	for _, obj := range objects {
		prefix := ""
		if dir != "" {
			prefix = dir + "/"
		}

		if obj.isDir {
			entries = append(entries, fs.NewDir(prefix+obj.remote, obj.modTime))
		} else {
			obj.remote = prefix + obj.remote
			entries = append(entries, obj)
		}
	}

	return entries, nil
}

func getObjectWithRetries(f* Fs, ctx context.Context, name string, pid int, token string) (entity api.Entity, err error) {
	f.pacer.Call(func() (bool, error) {
		entity, err = getObject(f, ctx, name, pid, token)
		return err != nil, fmt.Errorf("failed to get object(%s), err: %w", name, err)
	})

    return
}

func getObject(f *Fs, ctx context.Context, name string, pid int, token string) (api.Entity, error) {
	var newObject api.Entity

	pageNumber := 0
	numberOfEntities := maxEntitiesPerPage
	newObjectIsFound := false

	for numberOfEntities == maxEntitiesPerPage {
		pageNumber++
		request := makeSearchQuery("", pid, token, pageNumber)

		searchResponse := api.FileSearchRes{}
		err := f._GetUnmarshaledResponse(ctx, request, &searchResponse)
		if err != nil {
			return api.Entity{}, fmt.Errorf("unable to create new object: %w", err)
		}
		if searchResponse.Status != 1 {
			return api.Entity{}, fmt.Errorf("unable to create new object: %s", searchResponse.Message)
		}
		numberOfEntities = len(searchResponse.SearchData.Entities)

		for _, entity := range searchResponse.SearchData.Entities {
			if entity.Pid == pid && f.opt.Enc.ToStandardName(entity.Name) == name {
				newObject = entity
				newObjectIsFound = true
				break
			}
		}

		if newObjectIsFound {
			break
		}

		if pageNumber > 100000 {
			return api.Entity{}, fmt.Errorf("too many results")
		}
	}

	if !newObjectIsFound {
		return api.Entity{}, fs.ErrorObjectNotFound
	}

	return newObject, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
//
// If remote points to a directory then it should return
// ErrorIsDir if possible without doing any extra work,
// otherwise ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f._NewObject(ctx, remote, false)
}

func (f *Fs) _NewObject(ctx context.Context, remote string, allowDir bool) (fs.Object, error) {
	var newObject api.Entity
	var dir, name string

	fullPath := path.Join(f.root, remote)
	dir, name = splitDirAndName(fullPath)

	dirId, err := getIdByDir(f, ctx, dir)
	if err != nil {
		return nil, fs.ErrorObjectNotFound
	}

	newObject, err = getObject(f, ctx, name, dirId, f.opt.Token)
	if err != nil {
		return nil, err
	}

	if newObject == (api.Entity{}) {
		return nil, fs.ErrorObjectNotFound
	}

	if !allowDir && (newObject.Type == "dir" || newObject.Type == "sdir") {
		return nil, fs.ErrorIsDir
	}

	return &Object{
		fs:          f,
		remote:      remote,
		modTime:     time.Unix(newObject.Ctime, 0),
		fullURL:     newObject.URL,
		size:        int64(newObject.Size),
		isDir:       newObject.Type == "dir" || newObject.Type == "sdir",
		itemId:      newObject.ItemID,
		id: 	     newObject.ID,
		pid:         newObject.Pid,
		contentType: newObject.Type,
		subType:     newObject.SubType,
	}, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	fsObj, err := srcFs._NewObject(ctx, srcRemote, true)
	if err != nil {
		return fs.ErrorCantDirMove
	}

	srcObj := fsObj.(*Object)
	if !srcObj.isDir {
		return fs.ErrorCantDirMove
	}

	srcObj.remote = srcRemote
	_, err = f._MoveDir(ctx, srcObj, dstRemote)

	return err
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	var pdir, name string

	fullPath := path.Join(f.root, dir)
	if fullPath == "" {
		return nil
	}

	fullPath = strings.TrimPrefix(fullPath, "/")

	dirs := strings.Split(fullPath, "/")
	dirs = append([]string{""}, dirs...)

	for i, dirName := range dirs {
		pdir = path.Join(pdir, dirName)
		name = dirs[i+1]
		pid, err := getIdByDirWithRetries(f, ctx, pdir)
		if err != nil {
			log.Default().Printf("Error(%s) during making dir(%s)", err, dirName)
			return err
		}

		opts := rest.Opts{
			Method:  "GET",
			RootURL: "https://www.linkbox.to/api/open/folder_create",
			Parameters: url.Values{
				"name": []string{f.opt.Enc.FromStandardName(name)},
				"pid":  []string{strconv.Itoa(pid)},
				"token": []string{f.opt.Token},
				"isShare": []string{"0"},
				"canInvite": []string{"1"},
				"canShare": []string{"1"},
				"withBodyImg": []string{"1"},
				"desc": []string{""},
			},
		}

		response := api.CommonResponse{}
		err = f._GetUnmarshaledResponse(ctx, &opts, &response)
		if err != nil {
			return fmt.Errorf("err in response")
		}

		if i+1 == len(dirs)-1 {
			break
		}

		// response status 1501 means that directory already exists
		if response.Status != 1 && response.Status != 1501 {
			return fmt.Errorf("could not create dir[%s]: %s", dir, response.Message)
		}
	}

	// deflake when auto mkdir is needed in the middle of other fs apis
	_, err := getIdByDirWithRetries(f, ctx, fullPath)
	if err != nil {
		return fmt.Errorf("server hasn't reflect Mkdir(%s)", dir)
	}

	return nil
}

// purgeCheck removes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	objects, _ := parse(f, ctx, dir)
	if check && len(objects) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	fullPath := path.Join(f.root, dir)

	if fullPath == "" {
		return fs.ErrorDirNotFound
	}

	fullPath = strings.TrimPrefix(fullPath, "/")
	dirIds, err := getIdByDir(f, ctx, fullPath)

	if err != nil {
		return err
	}

	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_del",
		Parameters: url.Values{
			"dirIds": []string{strconv.Itoa(dirIds)},
			"token":  []string{f.opt.Token},
		},
	}

	response := api.CommonResponse{}
	err = f._GetUnmarshaledResponse(ctx, &opts, &response)

	if err != nil {
		return fmt.Errorf("err in response")
	}

	if response.Status != 1 {
		return fmt.Errorf("could not remove dir: %s", response.Message)
	}
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, true)
}

// Purge deletes all the files and the container
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, false)
}

func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

func (f *Fs) _ServerFolderEdit(ctx context.Context, dirId string, name string) error {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_edit",
		Parameters: url.Values{
			"dirId": []string{dirId},
			"name": []string{f.opt.Enc.FromStandardName(name)},
			"token": []string{f.opt.Token},
			"canShare": []string{"1"},
			"canInvite": []string{"1"},
			"change_avatar": []string{"0"},
			"desc": []string{""},
		},
	}

	response := api.CommonResponse{}
	err := f._GetUnmarshaledResponse(ctx, &opts, &response)
	if err != nil {
		return err
	} else if response.Status != 1 {
		return fmt.Errorf("error folder_edit: %s", response.Message)
	}

	return nil
}

func (f *Fs) _ServerFolderMove(ctx context.Context, dirId string, pid string) error {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_move",
		Parameters: url.Values{
			"dirIds": []string{dirId},
			"pid": []string{pid},
			"token": []string{f.opt.Token},
		},
	}

	response := api.CommonResponse{}
	err := f._GetUnmarshaledResponse(ctx, &opts, &response)
	if err != nil {
		return err
	} else if response.Status != 1 {
		return fmt.Errorf("error folder_move: %s", response.Message)
	}

	return nil
}

func (f *Fs) _MoveDir(ctx context.Context, srcObject *Object, dstRemote string) (fs.Object, error) {
	srcFullPath := path.Join(srcObject.fs.root, srcObject.remote)
	if srcFullPath == "" {
		return nil, fs.ErrorCantDirMove
	}
	srcFullPath = strings.TrimPrefix(srcFullPath, "/")

	dstFullPath := path.Join(f.root, dstRemote)
	if dstFullPath == "" {
		return nil, fs.ErrorCantDirMove
	}
	dstFullPath = strings.TrimPrefix(dstFullPath, "/")

	_, err := getIdByDir(f, ctx, dstFullPath)
	if err == nil {
		return nil, fs.ErrorDirExists
	}

	if path.Dir(srcFullPath) == path.Dir(dstFullPath) {
		err = f._ServerFolderEdit(ctx, strconv.Itoa(srcObject.id), path.Base(dstFullPath))
		if err != nil {
			return nil, fs.ErrorCantDirMove
		}
	} else if path.Base(srcFullPath) == path.Base(dstFullPath) {
		pid, err := getIdByDir(f, ctx, path.Dir(dstFullPath))
		if err == fs.ErrorDirNotFound && f.Mkdir(ctx, path.Dir(dstRemote)) == nil {
			pid, err = getIdByDir(f, ctx, path.Dir(dstFullPath))
		}
		if err != nil {
			return nil, fs.ErrorCantDirMove
		}
		err = f._ServerFolderMove(ctx, strconv.Itoa(srcObject.id), strconv.Itoa(pid))
		if err != nil {
			return nil, fs.ErrorCantDirMove
		}
	} else {
		tmpSrcPath := path.Dir(srcFullPath) + "/" + path.Base(dstFullPath)
		tmpDstPath := dstFullPath

		srcPath := tmpSrcPath
		dstPath := tmpDstPath
		for i := 0; i <= 100; i++ {
			if i > 0 {
				srcPath =  tmpSrcPath + "__" + strconv.Itoa(i)
				dstPath = tmpDstPath + "__" + strconv.Itoa(i)
			}

			// Ensure the temporary name doesn't exist in both src and dst
			_, err = getIdByDir(srcObject.fs, ctx, srcPath)
			if err == nil {
				continue
			}
			_, err = getIdByDir(f, ctx, dstPath)
			if err == nil {
				continue
			}

			// Get dst pid
			pid, err := getIdByDir(f, ctx, path.Dir(dstPath))
			if err == fs.ErrorDirNotFound && f.Mkdir(ctx, path.Dir(dstRemote)) == nil {
				pid, err = getIdByDirWithRetries(f, ctx, path.Dir(dstPath))
			}

			if err != nil {
				return nil, fs.ErrorCantDirMove
			}

			// Rename src to temporary src
			err = srcObject.fs._ServerFolderEdit(ctx, strconv.Itoa(srcObject.id), path.Base(srcPath))
			if err != nil {
				return nil, fs.ErrorCantDirMove
			}

			// Move temporary src to temmporary dst
			err = f._ServerFolderMove(ctx, strconv.Itoa(srcObject.id), strconv.Itoa(pid))
			if err != nil {
				return nil, fs.ErrorCantDirMove
			}

			// Rename temporary dst to dst if necessary
			if path.Base(dstPath) != path.Base(dstFullPath) {
				err = f._ServerFolderEdit(ctx, strconv.Itoa(srcObject.id), path.Base(dstFullPath))
				if err != nil {
					return nil, fs.ErrorCantDirMove
				}
			}

			break
		}
	}

	var newObject fs.Object
	f.pacer.Call(func() (bool, error) {
		newObject, err = f._NewObject(ctx, dstRemote, true)
		return err != nil, fmt.Errorf("server hasn't reflect MoveDir(%s), err: %w", dstRemote, err)
	})
	if err != nil {
		return nil, fs.ErrorCantDirMove
	}

	return newObject, nil
}

func (f *Fs) _MoveFile(ctx context.Context, srcObject *Object, remote string) (fs.Object, error) {
	srcFullPath := path.Join(srcObject.fs.root, srcObject.remote)
	srcFullPath = strings.TrimPrefix(srcFullPath, "/")

	dstFullPath := path.Join(f.root, remote)
	dstFullPath = strings.TrimPrefix(dstFullPath, "/")

	if path.Dir(srcFullPath) == path.Dir(dstFullPath) {
		err := f._ServerFileRename(ctx, srcObject.itemId, f.opt.Enc.FromStandardName(path.Base(dstFullPath)))
		if err != nil {
			return nil, fs.ErrorCantMove
		}
	} else if path.Base(srcFullPath) == path.Base(dstFullPath) {
		dirId, err := getIdByDir(f, ctx, path.Dir(dstFullPath))
		if err != nil {
			return nil, fs.ErrorCantMove
		}
		err = f._ServerFileMove(ctx, srcObject.itemId, strconv.Itoa(dirId))
		if err != nil {
			return nil, fs.ErrorCantMove
		}
	} else {
		tmpSrcRemote := path.Dir(srcObject.Remote()) + "/" + path.Base(remote)
		tmpDstRemote := remote
		tmpDstPath := dstFullPath

		srcRemote := tmpSrcRemote
		dstRemote := tmpDstRemote
		dstPath := tmpDstPath
		for i := 0; i <= 100; i++ {
			if i > 0 {
				srcRemote =  tmpSrcRemote + "__" + strconv.Itoa(i)
				dstRemote = tmpDstRemote + "__" + strconv.Itoa(i)
				dstPath = tmpDstPath + "__" + strconv.Itoa(i)
			}

			// Ensure the temporary name doesn't exist in both src and dst
			_, err := srcObject.fs.NewObject(ctx, srcRemote)
			if err == nil {
				continue
			}
			_, err = f.NewObject(ctx, dstRemote)
			if err == nil {
				continue
			}

			// Get dst pid
			pid, err := getIdByDir(f, ctx, path.Dir(dstPath))
			if err == fs.ErrorDirNotFound && f.Mkdir(ctx, path.Dir(dstRemote)) == nil {
				pid, err = getIdByDirWithRetries(f, ctx, path.Dir(dstPath))
			}

			if err != nil {
				return nil, fs.ErrorCantMove
			}

			// Rename src to temporary src
			err = srcObject.fs._ServerFileRename(
				ctx, srcObject.itemId, f.opt.Enc.FromStandardName(path.Base(srcRemote)))
			if err != nil {
				return nil, fs.ErrorCantMove
			}

			// Move temporary src to temmporary dst
			err = f._ServerFileMove(ctx, srcObject.itemId, strconv.Itoa(pid))
			if err != nil {
				return nil, fs.ErrorCantMove
			}

			// Rename temporary dst to dst if necessary
			if path.Base(dstPath) != path.Base(dstFullPath) {
				err = f._ServerFileRename(ctx, srcObject.itemId, f.opt.Enc.FromStandardName(path.Base(dstFullPath)))
				if err != nil {
					return nil, fs.ErrorCantMove
				}
			}

			break
		}
	}

	var err error
	var newObject fs.Object
	f.pacer.Call(func() (bool, error) {
		newObject, err = f.NewObject(ctx, remote)
		return err != nil, fmt.Errorf("server hasn't reflect MoveFile(%s), err: %w", remote, err)
	})
	if err != nil {
		return nil, fs.ErrorCantMove
	}

	return newObject, nil
}

func (f *Fs) _ServerFileRename(ctx context.Context, itemId string, name string) error {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/file_rename",
		Parameters: url.Values{
			"itemId": []string{itemId},
			"name": []string{name},
			"token": []string{f.opt.Token},
		},
	}

	response := api.CommonResponse{}
	err := f._GetUnmarshaledResponse(ctx, &opts, &response)
	if err != nil {
		return err
	} else if response.Status != 1 && response.Status != 1501 {
		return fmt.Errorf("error file_rename: %s", response.Message)
	}

	return nil
}

func (f *Fs) _ServerFileMove(ctx context.Context, itemId string, pid string) error {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/file_move",
		Parameters: url.Values{
			"itemIds": []string{itemId},
			"pid":    []string{pid},
			"token":  []string{f.opt.Token},
		},
	}

	response := api.CommonResponse{}
	err := f._GetUnmarshaledResponse(ctx, &opts, &response)
	if err != nil {
		return err
	} else if response.Status != 1 && response.Status != 1501 {
		return fmt.Errorf("error file_move: %s", response.Message)
	}

	return nil
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	if srcObj.isDir {
		return nil, fs.ErrorCantMove
	}

	return f._MoveFile(ctx, srcObj, remote)
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	downloadURL := o.fullURL
	if downloadURL == "" {
		_, name := splitDirAndName(o.Remote())
		newObject, err := getObject(o.fs, ctx, name, o.pid, o.fs.opt.Token)
		if err != nil {
			return nil, err
		}
		if newObject == (api.Entity{}) {
			return nil, fs.ErrorObjectNotFound
		}

		downloadURL = newObject.URL
	}

	fs.FixRangeOption(options, o.size)
	opts := rest.Opts{
		Method:  "GET",
		RootURL: downloadURL,
		Options: options,
	}

	res, err := o.fs._FetchDataWithRetries(ctx, &opts)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}

	return res.Body, nil
}

func (o *Object) UploadParts(in *io.Reader, metadata *api.FileUploadSessionRes, inSize int64, partSize int64) error {
	file, err := os.CreateTemp("", "linkbox_multipart_upload_*")
	if err != nil {
		return fmt.Errorf("failed to create multipart file: %w", err)
	}

	defer func(fd *os.File) {
		for _, err := range [...]error{fd.Close(), os.Remove(fd.Name())} {
			if err != nil {
				log.Default().Printf("failed to remove multipart file(%s): %s", file.Name(), err)
			}
		}
	}(file)

	_, err = io.Copy(file, *in)
	if err != nil {
		return fmt.Errorf("failed to copy data to temp file(%s): %w", file.Name(), err)
	}

	obsClient, err := obs.New(
		metadata.Data.Ak,
		metadata.Data.Sk,
		metadata.Data.Server,
		obs.WithSecurityToken(metadata.Data.SToken),
		// obs.WithHttpClient(o.fs.httpClient),
	)
	if err != nil {
		return fmt.Errorf("failed to create obs client: %w", err)
	}
	defer obsClient.Close()

	initiateMultipartUploadInput := &obs.InitiateMultipartUploadInput{}
	initiateMultipartUploadInput.Bucket = metadata.Data.Bucket
	initiateMultipartUploadInput.Key = metadata.Data.PoolPath
	initiateMultipartUploadOutput, err := obsClient.InitiateMultipartUpload(initiateMultipartUploadInput)
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	partCount := int(inSize / partSize)
	if inSize % partSize != 0 {
		partCount++
	}
	partChan := make(chan obs.Part, 5)

	for i := 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * partSize
		currPartSize := partSize
		if i+1 == partCount {
			currPartSize = inSize - offset
		}
		go func(index int, offset, partSize int64) {
			uploadPartInput := &obs.UploadPartInput{}
			uploadPartInput.Bucket = metadata.Data.Bucket
			uploadPartInput.Key = metadata.Data.PoolPath
			uploadPartInput.UploadId = initiateMultipartUploadOutput.UploadId
			uploadPartInput.SourceFile = file.Name()
			uploadPartInput.PartNumber = index
			uploadPartInput.Offset = offset
			uploadPartInput.PartSize = partSize
			uploadPartInputOutput, err := obsClient.UploadPart(uploadPartInput)
			if err == nil {
				partChan <- obs.Part{ETag: uploadPartInputOutput.ETag, PartNumber: uploadPartInputOutput.PartNumber}
			} else {
				partChan <- obs.Part{ETag: "", PartNumber: -1}
			}
		}(partNumber, offset, currPartSize)
	}

	parts := make([]obs.Part, 0, partCount)

	numFailedParts := 0
	for {
		part, ok := <-partChan
		if !ok {
			break
		}
		if part.PartNumber == -1 {
			numFailedParts++
		}
		parts = append(parts, part)
		if len(parts) == partCount {
			close(partChan)
		}
	}

	if numFailedParts > 0 {
		return fmt.Errorf("%d parts failed to upload", numFailedParts)
	}

	completeMultipartUploadInput := &obs.CompleteMultipartUploadInput{}
	completeMultipartUploadInput.Bucket = metadata.Data.Bucket
	completeMultipartUploadInput.Key = metadata.Data.PoolPath
	completeMultipartUploadInput.UploadId = initiateMultipartUploadOutput.UploadId
	completeMultipartUploadInput.Parts = parts
	_, err = obsClient.CompleteMultipartUpload(completeMultipartUploadInput)
	if err != nil {
		return fmt.Errorf("failed to complete part: %w", err)
	}

	return nil
}

func (o *Object) _MultipartUpload(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}

	tmpObject, err := o.fs.NewObject(ctx, o.Remote())

	if err == nil {
		_ = tmpObject.Remove(ctx)
	}

	h := md5.New()
	first10m := io.LimitReader(in, 10_485_760)
	first10mBytes, err := io.ReadAll(first10m)
	if err != nil {
		return err
	}

	if _, err := io.Copy(h, bytes.NewReader(first10mBytes)); err != nil {
		return err
	}

	vgroup := fmt.Sprintf("%x", h.Sum(nil)) + "_" + strconv.FormatInt(src.Size(), 10)
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/file/get_file_upload_session",
		Parameters: url.Values{
			"scene":           []string{"common"},
			"vgroupType":      []string{"md5_10m"},
			"vgroup":          []string{vgroup},
			"token":           []string{o.fs.accToken},
		},
	}

	fileUploadSessionRes := api.FileUploadSessionRes{}
	err = o.fs._GetUnmarshaledResponse(ctx, &opts, &fileUploadSessionRes)
	if err != nil {
		return err
	}

	switch fileUploadSessionRes.Status {
	case 1:
		file := io.MultiReader(bytes.NewReader(first10mBytes), in)

		// calculate part size according to the target o.fs.opt.MultipartPerUploadParts
		partSize := src.Size() / int64(o.fs.opt.MultipartPerUploadParts)
		if partSize < minPartSize {
			partSize = minPartSize
		} else if partSize > maxPartSize {
			partSize = maxPartSize
		}

		err = o.UploadParts(&file, &fileUploadSessionRes, src.Size(), partSize)
		if err != nil {
			return err
		}
	case 600:
		// Status means that we don't need to upload file
		// We need only to make second step
	default:
		return fmt.Errorf("get unexpected message from Linkbox: %s", fileUploadSessionRes.Message)
	}

	fullPath := path.Join(o.fs.root, o.Remote())
	fullPath = strings.TrimPrefix(fullPath, "/")

	pdir, name := splitDirAndName(fullPath)
	pdirToCreateIfNotExists, _ := splitDirAndName(o.Remote())

	pid, err := getIdByDir(o.fs, ctx, pdir)
	if err == fs.ErrorDirNotFound && o.fs.Mkdir(ctx, pdirToCreateIfNotExists) == nil {
		pid, err = getIdByDirWithRetries(o.fs, ctx, pdir)
	}

	if err != nil {
		return err
	}

	filename := o.fs.opt.Enc.FromStandardName(name)
	opts = rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/file/create_item",
		Parameters: url.Values{
			"diyName":    []string{filename},
			"filename":   []string{filename},
			"pid":        []string{strconv.Itoa(pid)},
			"vgroup":     []string{vgroup},
			"vgroupType": []string{"md5_10m"},
			"token":      []string{o.fs.accToken},
		},
	}

	createItemRes := api.CreateItemRes{}
	err = o.fs._GetUnmarshaledResponse(ctx, &opts, &createItemRes)
	if err != nil {
		return err
	}
	if createItemRes.Status != 1 {
		return fmt.Errorf("get bad status from linkbox: %s", createItemRes.Message)
	}

	newObject, err := getObjectWithRetries(o.fs, ctx, name, pid, o.fs.opt.Token)
	if err != nil || newObject == (api.Entity{}) {
		if createItemRes.Data.ItemID != "" {
			log.Default().Printf(
				"Fallback to directly update object from response as getObjectWithRetries(%s) failed",
				name)
			o.pid = pid
			o.itemId = createItemRes.Data.ItemID
			o.remote = o.Remote()
			o.size = src.Size()
			o.modTime = time.Now()
			return nil
		}
		log.Default().Printf(
			"Failed both getObjectWithRetries(%s) and direct object update", name)
	}

	o.pid = pid
	o.fullURL = newObject.URL
	o.id = newObject.ID
	o.itemId = newObject.ItemID
	o.isDir = newObject.Type == "dir" || newObject.Type == "sdir"
	o.contentType = newObject.Type
	o.subType = newObject.SubType
	o.remote = o.Remote()
	o.modTime = time.Unix(newObject.Ctime, 0)
	o.size = src.Size()

	return nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if o.fs.opt.MultipartPerUploadParts > 0 {
		return o._MultipartUpload(ctx, in, src, options...)
	}

	if src.Size() == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}

	tmpObject, err := o.fs.NewObject(ctx, o.Remote())

	if err == nil {
		_ = tmpObject.Remove(ctx)
	}

	h := md5.New()
	first10m := io.LimitReader(in, 10_485_760)
	first10mBytes, err := io.ReadAll(first10m)
	if err != nil {
		return err
	}

	if _, err := io.Copy(h, bytes.NewReader(first10mBytes)); err != nil {
		return err
	}

	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/get_upload_url",
		Parameters: url.Values{
			"fileMd5ofPre10m": []string{fmt.Sprintf("%x", h.Sum(nil))},
			"fileSize":        []string{strconv.FormatInt(src.Size(), 10)},
			"token":           []string{o.fs.opt.Token},
		},
	}

	getFistStepResult := api.UploadUrlResponse{}
	err = o.fs._GetUnmarshaledResponse(ctx, &opts, &getFistStepResult)
	if err != nil {
		return err
	}

	switch getFistStepResult.Status {
	case 1:
		file := io.MultiReader(bytes.NewReader(first10mBytes), in)

		opts = rest.Opts{
			Method:  "PUT",
			RootURL: getFistStepResult.Data.SignUrl,
			Body:    file,
		}

		res, err := o.fs._FetchDataWithRetries(ctx, &opts)
		if err != nil {
			return err
		}

		_, err = io.ReadAll(res.Body)
		if err != nil {
			return err
		}

	case 600:
		// Status means that we don't need to upload file
		// We need only to make second step
	default:
		return fmt.Errorf("get unexpected message from Linkbox: %s", getFistStepResult.Message)
	}

	fullPath := path.Join(o.fs.root, o.Remote())
	fullPath = strings.TrimPrefix(fullPath, "/")

	pdir, name := splitDirAndName(fullPath)
	pdirToCreateIfNotExists, _ := splitDirAndName(o.Remote())

	pid, err := getIdByDir(o.fs, ctx, pdir)
	if err == fs.ErrorDirNotFound && o.fs.Mkdir(ctx, pdirToCreateIfNotExists) == nil {
		pid, err = getIdByDirWithRetries(o.fs, ctx, pdir)
	}

	if err != nil {
		return err
	}

	opts = rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_upload_file",
		Parameters: url.Values{
			"fileMd5ofPre10m": []string{fmt.Sprintf("%x", h.Sum(nil))},
			"fileSize":        []string{strconv.FormatInt(src.Size(), 10)},
			"pid":             []string{strconv.Itoa(pid)},
			"diyName":         []string{o.fs.opt.Enc.FromStandardName(name)},
			"token":           []string{o.fs.opt.Token},
		},
	}

	getSecondStepResult := api.UploadFileResponse{}
	err = o.fs._GetUnmarshaledResponse(ctx, &opts, &getSecondStepResult)
	if err != nil {
		return err
	}
	if getSecondStepResult.Status != 1 {
		return fmt.Errorf("get bad status from linkbox: %s", getSecondStepResult.Msg)
	}

	newObject, err := getObjectWithRetries(o.fs, ctx, name, pid, o.fs.opt.Token)
	if err != nil || newObject == (api.Entity{}) {
		if getSecondStepResult.Data.ItemID != "" {
			log.Default().Printf(
				"Fallback to directly update object from response as getObjectWithRetries(%s) failed",
				name)
			o.pid = pid
			o.itemId = getSecondStepResult.Data.ItemID
			o.remote = o.Remote()
			o.size = src.Size()
			o.modTime = time.Now()
			return nil
		}
		log.Default().Printf(
			"Failed both getObjectWithRetries(%s) and direct object update", name)
	}

	o.pid = pid
	o.fullURL = newObject.URL
	o.id = newObject.ID
	o.itemId = newObject.ItemID
	o.isDir = newObject.Type == "dir" || newObject.Type == "sdir"
	o.contentType = newObject.Type
	o.subType = newObject.SubType
	o.remote = o.Remote()
	o.modTime = time.Unix(newObject.Ctime, 0)
	o.size = src.Size()

	return nil
}

// Removes this object
func (o *Object) Remove(ctx context.Context) error {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/file_del",
		Parameters: url.Values{
			"itemIds": []string{o.itemId},
			"token":   []string{o.fs.opt.Token},
		},
	}

	requstResult := api.UploadUrlResponse{}
	err := o.fs._GetUnmarshaledResponse(ctx, &opts, &requstResult)
	if err != nil {
		return err
	}

	if requstResult.Status != 1 {
		return fmt.Errorf("get unexpected message from Linkbox: %s", requstResult.Message)
	}

	// deflake rmdir-right-after-remove
	o.fs.pacer.Call(func() (bool, error) {
		_, err = o.fs.NewObject(ctx, o.Remote())
		if (err == fs.ErrorObjectNotFound) {
			return false, nil
		}
		return true, fmt.Errorf("server hasn't reflected file(%s) removal", o.Remote())
	})

	return nil
}

// ModTime returns the modification time of the remote http file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Remote the name of the remote HTTP file, relative to the fs root
func (o *Object) Remote() string {
	return o.remote
}

// Size returns the size in bytes of the remote http file
func (o *Object) Size() int64 {
	return o.size
}

// String returns the URL to the remote HTTP file
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Fs is the filesystem this remote http file object is located within
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns "" since HTTP (in Go or OpenSSH) doesn't support remote calculation of hashes
func (o *Object) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	if o.contentType != "" && o.subType != "" {
		t := o.contentType
		if t == "doc" {
			t = "text"
		}
		st := o.subType
		if st == "txt" {
			st = "plain"
		}
		return t + "/" + st
	}
	return ""
}

// Storable returns whether the remote http file is a regular file
// (not a directory, symbolic link, block device, character device, named pipe, etc.)
func (o *Object) Storable() bool {
	return true
}

// Info provides a read only interface to information about a filesystem.
// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Name of the remote (as passed into NewFs)
// Name returns the configured name of the file system
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("Linkbox root '%s'", f.root)
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// Returns the supported hash types of the filesystem
// Hashes returns hash.HashNone to indicate remote hashing is unavailable
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: src.Remote(),
		size:   src.Size(),
	}
	err := o.Update(ctx, in, src, options...)
	return o, err
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = &Fs{}
	_ fs.DirMover  = &Fs{}
	_ fs.Mover     = &Fs{}
	_ fs.Purger    = &Fs{}
	_ fs.Abouter   = &Fs{}
	_ fs.Object    = &Object{}
	_ fs.MimeTyper = &Object{}
)
