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
	"path"
	"strconv"
	"strings"
	"time"

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

var (

)

const (
	maxEntitiesPerPage = 64
	minSleep           = 20 * time.Millisecond
	maxSleep           = 4 * time.Second
	decayConstant      = 2
)

func init() {
	fsi := &fs.RegInfo{
		Name:        "linkbox",
		Description: "Linkbox",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "token",
			Help:     "Token from https://www.linkbox.to/admin/account",
			Required: true,
		}, {
			Name:      "email",
			Help:      "The email for https://www.linkbox.to/api/user/login_email?email={email}",
			Sensitive: true,
		}, {
			Name:       "password",
			Help:       "The password for https://www.linkbox.to/api/user/login_email?pwd={password}",
			IsPassword: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: encoder.EncodeInvalidUtf8,
		}},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Token string             `config:"token"`
	Email string             `config:"email"`
	Password string          `config:"password"`
	Enc encoder.MultiEncoder `config:"encoding"`
}

// Fs stores the interface to the remote Linkbox files
type Fs struct {
	name     string
	root     string
	opt      Options        // options for this backend
	features *fs.Features   // optional features
	ci       *fs.ConfigInfo // global config
	srv      *rest.Client   // the connection to the server
	pacer    *fs.Pacer      // pacer for API calls
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

	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		ci:   ci,
		srv:   rest.NewClient(fshttp.NewClient(ctx)),
		pacer: fs.NewPacer(
			ctx, pacer.NewDefault(pacer.MinSleep(minSleep),
			pacer.MaxSleep(maxSleep),
			pacer.DecayConstant(decayConstant))),
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

type Entity struct {
	Type    string `json:"type"`
	SubType string `json:"sub_type"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Ctime   int64  `json:"ctime"`
	Size    int    `json:"size"`
	ID      int    `json:"id"`
	Pid     int    `json:"pid"`
	ItemID  string `json:"item_id"`
}
type Data struct {
	Entities []Entity `json:"list"`
}
type FileSearchRes struct {
	SearchData Data   `json:"data"`
	Status     int    `json:"status"`
	Message    string `json:"msg"`
}

type LoginRes struct {
	Data struct {
		Avatar       string `json:"avatar"`
		Nickname     string `json:"nickname"`
		OpenID       string `json:"openId"`
		RefreshToken string `json:"refresh_token"`
		Token        string `json:"token"`
		UID          int    `json:"uid"`
		UserInfo     struct {
			APIKey         string `json:"api_key"`
			AutoRenew      bool   `json:"auto_renew"`
			Avatar         string `json:"avatar"`
			BuyP7          bool   `json:"buy_p7"`
			Email          string `json:"email"`
			FavFlag        bool   `json:"fav_flag"`
			GroupPLimitCnt int    `json:"group_p_limit_cnt"`
			IsTry          bool   `json:"is_try"`
			Nickname       string `json:"nickname"`
			OpenID         string `json:"open_id"`
			SetPrivacy     int    `json:"set_privacy"`
			SizeCap        int64  `json:"size_cap"`
			SizeCurr       int    `json:"size_curr"`
			State          int    `json:"state"`
			SubAutoRenew   int    `json:"sub_auto_renew"`
			SubOrderID     int    `json:"sub_order_id"`
			SubPid         int    `json:"sub_pid"`
			SubSource      int    `json:"sub_source"`
			SubType        int    `json:"sub_type"`
			UID            int    `json:"uid"`
			VipEnd         int    `json:"vip_end"`
			VipLv          int    `json:"vip_lv"`
		} `json:"userInfo"`
	} `json:"data"`
	Status int `json:"status"`
}

func getIdByDirWithRetries(f *Fs, ctx context.Context, dir string) (pid int, err error) {
	f.pacer.Call(func() (bool, error) {
		pid, err = getIdByDir(f, ctx, dir)
		if err != nil {
			log.Default().Printf("Retrying get dirId(%s), err: %s", dir, err)
		}
		return err != nil, err
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
			requestURL := makeSearchQuery("", pid, f.opt.Token, pageNumber)
			responseResult := FileSearchRes{}
			err := f._GetUnmarshaledResponse(ctx, requestURL, &responseResult)
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

func (f *Fs) _FetchDataWithRetries(client *http.Client, req *http.Request) (resp *http.Response, err error) {
	// var resp *http.Response
	// opts := rest.Opts{
	// 	Method:  req.Method,
	// 	RootURL: req.URL.Scheme + "://" + req.URL.Host,
	// 	Path:    req.URL.Path,
	// 	Options: req.URL.RawQuery,
	// }
	// err = o.fs.pacer.Call(func() (bool, error) {
	// 	resp, err = o.fs.srvRest.Call(ctx, &opts)
	// 	return o.fs.shouldRetry(ctx, err)
	// })

	f.pacer.Call(func() (bool, error) {
		resp, err = client.Do(req)
		if err != nil {
			log.Default().Printf("Retrying fetch data, err: %s", err)
		}
		return err != nil, err
	})

    return
}

func (f *Fs) _GetUnmarshaledResponse(ctx context.Context, url string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	res, err := f._FetchDataWithRetries(fshttp.NewClient(ctx), req)
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

func makeSearchQuery(name string, pid int, token string, pageNubmer int) string {
	requestURL, _ := url.Parse("https://www.linkbox.to/api/open/file_search")
	q := requestURL.Query()
	q.Set("name", name)
	q.Set("pid", strconv.Itoa(pid))
	q.Set("token", token)
	q.Set("pageNo", strconv.Itoa(pageNubmer))
	q.Set("pageSize", strconv.Itoa(maxEntitiesPerPage))
	requestURL.RawQuery = q.Encode()
	return requestURL.String()
}

func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	if (f.opt.Email == "") || (f.opt.Password == "") {
		return nil, fmt.Errorf("email and password are required")
	}

	pass, err := obscure.Reveal(f.opt.Password)
	if err != nil {
		return nil, fmt.Errorf("Error decoding password, obscure it?: %w", err)
	}

	requestURL, _ := url.Parse("https://www.linkbox.to/api/user/login_email")
	q := requestURL.Query()
	q.Set("email", f.opt.Email)
	q.Set("pwd", pass)

	requestURL.RawQuery = q.Encode()
	response := LoginRes{}

	err = f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)
	if err != nil || response.Status != 1 {
		return nil, fmt.Errorf("Error login: %w", err)
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
	var responseResult FileSearchRes
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
		requestURL := makeSearchQuery("", pid, f.opt.Token, pageNumber)

		responseResult = FileSearchRes{}
		err = f._GetUnmarshaledResponse(ctx, requestURL, &responseResult)
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

func getObjectWithRetries(f* Fs, ctx context.Context, name string, pid int, token string) (entity Entity, err error) {
	f.pacer.Call(func() (bool, error) {
		entity, err = getObject(f, ctx, name, pid, token)
		if err != nil {
			log.Default().Printf("Retrying get object(%s), err: %s", name, err)
		}
		return err != nil, err
	})

    return
}

func getObject(f *Fs, ctx context.Context, name string, pid int, token string) (Entity, error) {
	var newObject Entity

	pageNumber := 0
	numberOfEntities := maxEntitiesPerPage
	newObjectIsFound := false

	for numberOfEntities == maxEntitiesPerPage {
		pageNumber++
		requestURL := makeSearchQuery("", pid, token, pageNumber)

		searchResponse := FileSearchRes{}
		err := f._GetUnmarshaledResponse(ctx, requestURL, &searchResponse)
		if err != nil {
			return Entity{}, fmt.Errorf("unable to create new object: %w", err)
		}
		if searchResponse.Status != 1 {
			return Entity{}, fmt.Errorf("unable to create new object: %s", searchResponse.Message)
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
			return Entity{}, fmt.Errorf("too many results")
		}
	}

	if !newObjectIsFound {
		return Entity{}, fs.ErrorObjectNotFound
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
	var newObject Entity
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

	if newObject == (Entity{}) {
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

		//"https://www.linkbox.to/api/open/folder_create"
		requestURL, _ := url.Parse("https://www.linkbox.to/api/open/folder_create")
		q := requestURL.Query()
		q.Set("name", f.opt.Enc.FromStandardName(name))
		q.Set("pid", strconv.Itoa(pid))
		q.Set("token", f.opt.Token)
		q.Set("isShare", "0")
		q.Set("canInvite", "1")
		q.Set("canShare", "1")
		q.Set("withBodyImg", "1")
		q.Set("desc", "")

		requestURL.RawQuery = q.Encode()
		response := getResponse{}

		err = f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)
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

	//"https://www.linkbox.to/api/open/folder_del"
	requestURL, _ := url.Parse("https://www.linkbox.to/api/open/folder_del")
	q := requestURL.Query()
	q.Set("dirIds", strconv.Itoa(dirIds))
	q.Set("token", f.opt.Token)

	requestURL.RawQuery = q.Encode()

	response := getResponse{}
	err = f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)

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
	var requestURL *url.URL
	var q url.Values

	//"https://www.linkbox.to/api/open/folder_edit"
	requestURL, _ = url.Parse("https://www.linkbox.to/api/open/folder_edit")
	q = requestURL.Query()
	q.Set("dirId", dirId)
	q.Set("name", f.opt.Enc.FromStandardName(name))
	q.Set("token", f.opt.Token)
	q.Set("canShare", "1")
	q.Set("canInvite", "1")
	q.Set("change_avatar", "0")
	q.Set("desc", "")

	requestURL.RawQuery = q.Encode()
	response := getResponse{}

	err := f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)
	if err != nil {
		return err
	} else if response.Status != 1 {
		return fmt.Errorf("Error folder_edit: %s", response.Message)
	}

	return nil
}

func (f *Fs) _ServerFolderMove(ctx context.Context, dirId string, pid string) error {
	var requestURL *url.URL
	var q url.Values

	//"https://www.linkbox.to/api/open/folder_move"
	requestURL, _ = url.Parse("https://www.linkbox.to/api/open/folder_move")
	q = requestURL.Query()
	q.Set("dirIds", dirId)
	q.Set("pid", pid)
	q.Set("token", f.opt.Token)

	requestURL.RawQuery = q.Encode()
	response := getResponse{}

	err := f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)
	if err != nil {
		return err
	} else if response.Status != 1 {
		return fmt.Errorf("Error folder_move: %s", response.Message)
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

	newObject, err := f._NewObject(ctx, dstRemote, true)
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

	newObject, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, fs.ErrorCantMove
	}

	return newObject, nil
}

func (f *Fs) _ServerFileRename(ctx context.Context, itemId string, name string) error {
	var requestURL *url.URL
	var q url.Values

	//"https://www.linkbox.to/api/open/file_rename"
	requestURL, _ = url.Parse("https://www.linkbox.to/api/open/file_rename")
	q = requestURL.Query()
	q.Set("itemId", itemId)
	q.Set("name", name)
	q.Set("token", f.opt.Token)

	requestURL.RawQuery = q.Encode()
	response := getResponse{}

	err := f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)
	if err != nil {
		return err
	} else if response.Status != 1 && response.Status != 1501 {
		return fmt.Errorf("Error file_rename: %s", response.Message)
	}

	return nil
}

func (f *Fs) _ServerFileMove(ctx context.Context, itemId string, pid string) error {
	var requestURL *url.URL
	var q url.Values

	//"https://www.linkbox.to/api/open/file_move"
	requestURL, _ = url.Parse("https://www.linkbox.to/api/open/file_move")
	q = requestURL.Query()
	q.Set("itemIds", itemId)
	q.Set("pid", pid)
	q.Set("token", f.opt.Token)

	requestURL.RawQuery = q.Encode()
	response := getResponse{}

	err := f._GetUnmarshaledResponse(ctx, requestURL.String(), &response)
	if err != nil {
		return err
	} else if response.Status != 1 && response.Status != 1501 {
		return fmt.Errorf("Error file_move: %s", response.Message)
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
		if newObject == (Entity{}) {
			return nil, fs.ErrorObjectNotFound
		}

		downloadURL = newObject.URL
	}
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}

	fs.FixRangeOption(options, o.size)
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	if o.size == 0 {
		// Don't supply range requests for 0 length objects as they always fail
		delete(req.Header, "Range")
	}

	res, err := o.fs._FetchDataWithRetries(fshttp.NewClient(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}

	return res.Body, nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
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

	requestURL, _ := url.Parse("https://www.linkbox.to/api/open/get_upload_url")
	q := requestURL.Query()
	q.Set("fileMd5ofPre10m", fmt.Sprintf("%x", h.Sum(nil)))
	q.Set("fileSize", strconv.FormatInt(src.Size(), 10))
	q.Set("token", o.fs.opt.Token)

	requestURL.RawQuery = q.Encode()
	getFistStepResult := getUploadUrlResponse{}
	err = o.fs._GetUnmarshaledResponse(ctx, requestURL.String(), &getFistStepResult)
	if err != nil {
		return err
	}

	switch getFistStepResult.Status {
	case 1:
		file := io.MultiReader(bytes.NewReader(first10mBytes), in)
		req, err := http.NewRequestWithContext(ctx, "PUT", getFistStepResult.Data.SignUrl, file)

		if err != nil {
			return err
		}

		res, err := o.fs._FetchDataWithRetries(fshttp.NewClient(ctx), req)
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

	requestURL, err = url.Parse("https://www.linkbox.to/api/open/folder_upload_file")
	if err != nil {
		return err
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

	q = requestURL.Query()
	q.Set("fileMd5ofPre10m", fmt.Sprintf("%x", h.Sum(nil)))
	q.Set("fileSize", strconv.FormatInt(src.Size(), 10))
	q.Set("pid", strconv.Itoa(pid))
	q.Set("diyName", o.fs.opt.Enc.FromStandardName(name))
	q.Set("token", o.fs.opt.Token)

	requestURL.RawQuery = q.Encode()
	getSecondStepResult := getUploadFileResponse{}
	err = o.fs._GetUnmarshaledResponse(ctx, requestURL.String(), &getSecondStepResult)
	if err != nil {
		return err
	}
	if getSecondStepResult.Status != 1 {
		return fmt.Errorf("get bad status from linkbox: %s", getSecondStepResult.Msg)
	}

	newObject, err := getObjectWithRetries(o.fs, ctx, name, pid, o.fs.opt.Token)
	if err != nil || newObject == (Entity{}) {
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
	//https://www.linkbox.to/api/open/file_del

	requestURL, err := url.Parse("https://www.linkbox.to/api/open/file_del")
	if err != nil {
		log.Fatal(err)
	}
	q := requestURL.Query()
	q.Set("itemIds", o.itemId)
	q.Set("token", o.fs.opt.Token)
	requestURL.RawQuery = q.Encode()
	requstResult := getUploadUrlResponse{}
	err = o.fs._GetUnmarshaledResponse(ctx, requestURL.String(), &requstResult)
	if err != nil {
		return err
	}

	if requstResult.Status != 1 {
		return fmt.Errorf("get unexpected message from Linkbox: %s", requstResult.Message)
	}

	// Deflake rmdir-right-after-remove
	o.fs.pacer.Call(func() (bool, error) {
		_, err = o.fs.NewObject(ctx, o.Remote())
		if (err == fs.ErrorObjectNotFound) {
			return false, nil
		}
		return true, fmt.Errorf("Retrying as server hasn't reflected file(%s) removal", o.Remote())


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

/*
	{
	  "data": {
	    "signUrl": "http://xx -- Then CURL PUT your file with sign url "
	  },
	  "msg": "please use this url to upload (PUT method)",
	  "status": 1
	}
*/
type getResponse struct {
	Message string `json:"msg"`
	Status  int    `json:"status"`
}

type getUploadUrlData struct {
	SignUrl string `json:"signUrl"`
}

type getUploadUrlResponse struct {
	Data getUploadUrlData `json:"data"`
	getResponse
}

type getUploadFileResponse struct {
	Data struct {
		ItemID string `json:"itemId"`
	} `json:"data"`
	Msg    string `json:"msg"`
	Status int    `json:"status"`
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
