// Package linkbox provides an interface to the linkbox.to Cloud storage system.
//
// API docs: https://www.linkbox.to/api-docs
package linkbox

/*
   Extras
   - PublicLink - NO - sharing doesn't share the actual file, only a page with it on
   - Move - YES - have Move and Rename file APIs so is possible
   - MoveDir - NO - probably not possible - have Move but no Rename
*/

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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/huaweicloud-sdk-go-obs/obs"
	"github.com/rclone/rclone/backend/linkbox/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"storj.io/common/readcloser"
)

const (
	maxEntitiesPerPage                 = 1024
	minSleep                           = 200 * time.Millisecond // Server sometimes reflects changes slowly
	maxSleep                           = 2 * time.Second
	decayConstant                      = 2
	multipartResponseHeaderTimeoutSec  = 90
	maxPartSize                        = 5 * 1_024 * 1_024 * 1_024 // SDK developer guide: max 5 GB
	minPartSize                        = 1 // SDK developer guide: min 100 KB?
	defaultPartSize                    = 6 * 1_024 * 1_024
	maxPerUploadParts                  = 10_000
	multipartMaxBufferSize             = 200 * 1024 * 1024
	multipartTxConcurrency             = 32
	multipartRxConcurrency             = 16
	minDownloadPartSize                = 1 * 1_024 * 1_024
	maxTxRxRetries                     = 3
	minTxRxRetrySleep                  = 20 * time.Millisecond
	maxTxRxRetrySleep                  = 500 * time.Millisecond
	multipartTxIntegrity               = false
	multipartTxPacerNumberScale        = 3
	rootID                             = "0"
	userAgent                          = "okhttp/4.9.3"
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
			Name:       "multipart_tx_concurrency",
			Help:       "The target concurrency of multipart uploading. 0 to disable",
			Default:    multipartTxConcurrency,
			Advanced:   true,
		}, {
			Name:       "multipart_tx_part_size",
			Help:       "The part size of multipart uploading",
			Default:    defaultPartSize,
			Advanced:   true,
		}, {
			Name:       "multipart_tx_max_buffer_size",
			Help:       "The max buffer size of multipart uploading. Buffer is per transfer determined by rclone --transfers",
			Default:    multipartMaxBufferSize,
			Advanced:   true,
		}, {
			Name:       "multipart_tx_integrity",
			Help:       "Whether to check multipart upload integrity, may impact throughput to some extent",
			Default:    multipartTxIntegrity,
			Advanced:   true,
		}, {
			Name:       "multipart_rx_concurrency",
			Help:       "The target concurrency of multipart donwloading. 0 to disable",
			Default:    multipartRxConcurrency,
			Advanced:   true,
		}, {
			Name:       "multipart_response_header_timeout",
			Help:       "The timeout of waiting for response header of uploading parts",
			Default:    multipartResponseHeaderTimeoutSec,
			Advanced:   true,
		}, {
			Name:       "user_agent",
			Help:       `HTTP user agent used internally by client.
Defaults to "rclone/VERSION" or "--user-agent" provided on command line.`,
			Default:    userAgent,
			Advanced:   true,
		}},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Token string                       `config:"token"`
	Email string                       `config:"email"`
	Password string                    `config:"password"`
	Enc encoder.MultiEncoder           `config:"encoding"`
	MultipartTxConcurrency int         `config:"multipart_tx_concurrency"`
	MultipartTxPartSize   int64        `config:"multipart_tx_part_size"`
	MultipartTxMaxBufferSize int64     `config:"multipart_tx_max_buffer_size"`
	MultipartTxIntegrity   bool        `config:"multipart_tx_integrity"`
	MultipartRxConcurrency int         `config:"multipart_rx_concurrency"`
	MultipartResponseHeaderTimeout int `config:"multipart_response_header_timeout"`
	UserAgent string                   `config:"user_agent"`
}

// Fs stores the interface to the remote Linkbox files
type Fs struct {
	name       string
	root       string
	opt        Options          // options for this backend
	features   *fs.Features     // optional features
	ci         *fs.ConfigInfo   // global config
	downloader *http.Client     // multipart downloader
	srv        *rest.Client     // the connection to the server
	pacer      *fs.Pacer        // pacer for API calls
	txPacers   []*fs.Pacer      // pacers for multipart uploads
	rxPacers   []*fs.Pacer      // pacers for multipart downloads
	accToken   string           // account token
	dirCache *dircache.DirCache // Map of directory path to directory id
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
	root = strings.Trim(root, "/")
	// Parse config into Options struct

	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	ci := fs.GetConfig(ctx)

	f := &Fs{
		name: name,
		opt:  *opt,
		root: root,
		ci:   ci,
		// srv:  rest.NewClient(fshttp.NewClient(ctx)),
		// downloader: fshttp.NewClient(ctx),
		pacer: fs.NewPacer(
			ctx, pacer.NewDefault(pacer.MinSleep(minSleep),
			pacer.MaxSleep(maxSleep))),
		txPacers: make([]*fs.Pacer, 0),
		rxPacers: make([]*fs.Pacer, 0),
		accToken: "",
	}

	// Adjust client config and pass it attached to context
	clientCtx, clientCfg := fs.AddConfig(ctx)
	if opt.UserAgent != "" {
		clientCfg.UserAgent = opt.UserAgent
	}
	f.downloader = fshttp.NewClient(clientCtx)
	f.srv = rest.NewClient(fshttp.NewClient(clientCtx))

	f.dirCache = dircache.New(root, rootID, f)

	for i := 0; i < f.opt.MultipartTxConcurrency * multipartTxPacerNumberScale; i++ {
		f.txPacers = append(f.txPacers, fs.NewPacer(
			ctx, pacer.NewDefault(pacer.MinSleep(minTxRxRetrySleep),
			pacer.MaxSleep(maxTxRxRetrySleep))))
	}

	for i := 0; i < f.opt.MultipartRxConcurrency; i++ {
		f.rxPacers = append(f.rxPacers, fs.NewPacer(
			ctx, pacer.NewDefault(pacer.MinSleep(minTxRxRetrySleep),
			pacer.MaxSleep(maxTxRxRetrySleep))))
	}

	f._UpdateAccountToken(ctx)

	// Account token is the prerequisite for OBS multipart uploads
	// If it is not available, fall back to the default API upload mode
	if f.accToken == "" {
		f.opt.MultipartTxConcurrency = 0
	}

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
	}).Fill(ctx, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

func (f *Fs) _FetchWithRetries(ctx context.Context, opts *rest.Opts) (resp *http.Response, err error) {
	f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.Call(ctx, opts)
		return f.shouldRetry(ctx, resp, err)
	})

    return
}

func (f *Fs) _GetUnmarshaledResponse(ctx context.Context, opts *rest.Opts, result interface{}) error {
	res, err := f._FetchWithRetries(ctx, opts)
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

// Set an object info from an entity
func (o *Object) set(e *api.Entity) {
	o.modTime = time.Unix(e.Ctime, 0)
	o.contentType = e.Type
	o.subType = e.SubType
	o.size = int64(e.Size)
	o.fullURL = e.URL
	o.isDir = IsDir(e)
	o.id = e.ID
	o.itemId = e.ItemID
	o.pid = e.Pid
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
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	_, err = f.listAll(ctx, directoryID, "", func(entity *api.Entity) bool {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(entity.Name))
		if IsDir(entity) {
			id := itoa(entity.ID)
			modTime := time.Unix(entity.Ctime, 0)
			d := fs.NewDir(remote, modTime).SetID(id).SetParentID(itoa(entity.Pid))
			entries = append(entries, d)
			// cache the directory ID for later lookups
			f.dirCache.Put(remote, id)
		} else {
			o := &Object{
				fs:     f,
				remote: remote,
			}
			o.set(entity)
			entries = append(entries, o)
		}
		return false
	})
	if err != nil {
		return nil, err
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

	query := name
	query = strings.TrimSpace(query) // search doesn't like spaces
	if !searchOK.MatchString(query) {
		// If query isn't good then do an unbounded search
		query = ""
	}

	for numberOfEntities == maxEntitiesPerPage {
		pageNumber++
		request := makeSearchQuery(query, pid, token, pageNumber)

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

func IsDir(entity *api.Entity) bool {
	return entity.Type == "dir" || entity.Type == "sdir"
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
	leaf, dirId, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}

	pid, _ := strconv.Atoi(dirId)
	newObject, err = getObject(f, ctx, leaf, pid, f.opt.Token)
	if err != nil {
		return nil, err
	}

	if newObject == (api.Entity{}) {
		return nil, fs.ErrorObjectNotFound
	}

	if !allowDir && IsDir(&newObject) {
		return nil, fs.ErrorIsDir
	}

	return &Object{
		fs:          f,
		remote:      remote,
		modTime:     time.Unix(newObject.Ctime, 0),
		fullURL:     newObject.URL,
		size:        int64(newObject.Size),
		isDir:       IsDir(&newObject),
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
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Turn 64 bit int to string
func itoa64(i int64) string {
	return strconv.FormatInt(i, 10)
}

// Turn int to string
func itoa(i int) string {
	return itoa64(int64(i))
}

// Returned from "folder_create"
type folderCreateRes struct {
	api.CommonResponse
	Data struct {
		DirID int64 `json:"dirId"`
	} `json:"data"`
}

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*api.Entity) bool

// Search is a bit fussy about which characters match
//
// If the name doesn't match this then do an dir list instead
var searchOK = regexp.MustCompile(`^[a-zA-Z0-9_ .]+$`)

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
//
// If you set name then search ignores dirID. name is a substring
// search also so name="dir" matches "sub dir" also. This filters it
// down so it only returns items in dirID
func (f *Fs) listAll(ctx context.Context, dirID string, name string, fn listAllFn) (found bool, err error) {
	var (
		pageNumber       = 0
		numberOfEntities = maxEntitiesPerPage
	)
	name = strings.TrimSpace(name) // search doesn't like spaces
	if !searchOK.MatchString(name) {
		// If name isn't good then do an unbounded search
		name = ""
	}
OUTER:
	for numberOfEntities == maxEntitiesPerPage {
		pageNumber++
		pid, _ := strconv.Atoi(dirID)
		request := makeSearchQuery(name, pid, f.opt.Token, pageNumber)
		responseResult := api.FileSearchRes{}
		err := f._GetUnmarshaledResponse(ctx, request, &responseResult)
		if err != nil {
			return false, fmt.Errorf("getting files failed: %w", err)
		}

		numberOfEntities = len(responseResult.SearchData.Entities)

		for _, entity := range responseResult.SearchData.Entities {
			if itoa(entity.Pid) != dirID {
				// when name != "" this returns from all directories, so ignore not this one
				continue
			}
			if fn(&entity) {
				found = true
				break OUTER
			}
		}
		if pageNumber > 100000 {
			return false, fmt.Errorf("too many results")
		}
	}
	return found, nil
}

// FindLeaf finds a directory of name leaf in the folder with ID directoryID
func (f *Fs) FindLeaf(ctx context.Context, directoryID, leaf string) (directoryIDOut string, found bool, err error) {
	// Find the leaf in directoryID
	found, err = f.listAll(ctx, directoryID, leaf, func(entity *api.Entity) bool {
		if IsDir(entity) && f.opt.Enc.ToStandardName(entity.Name) == leaf {
			directoryIDOut = itoa(entity.ID)
			return true
		}
		return false
	})
	return directoryIDOut, found, err
}

// CreateDir makes a directory with dirID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, dirID, leaf string) (newID string, err error) {
	// fs.Debugf(f, "CreateDir(%q, %q)\n", dirID, leaf)
	opts := &rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_create",
		Parameters: url.Values{
			"token":       {f.opt.Token},
			"name":        {f.opt.Enc.FromStandardName(leaf)},
			"pid":         {dirID},
			"isShare":     {"0"},
			"canInvite":   {"1"},
			"canShare":    {"1"},
			"withBodyImg": {"1"},
			"desc":        {""},
		},
	}

	response := folderCreateRes{}
	err = f._GetUnmarshaledResponse(ctx, opts, &response)
	if err != nil {
		// response status 1501 means that directory already exists
		if response.Status == 1501 {
			return newID, fmt.Errorf("couldn't find already created directory: %w", fs.ErrorDirNotFound)
		}
		return newID, fmt.Errorf("CreateDir failed: %w", err)

	}
	if response.Data.DirID == 0 {
		return newID, fmt.Errorf("API returned 0 for ID of newly created directory")
	}
	return itoa64(response.Data.DirID), nil
}

// purgeCheck removes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	if check {
		entries, err := f.List(ctx, dir)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return fs.ErrorDirectoryNotEmpty
		}
	}

	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_del",
		Parameters: url.Values{
			"dirIds": []string{directoryID},
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

	f.dirCache.FlushDir(dir)

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
	defer srcObject.fs.dirCache.FlushDir(srcObject.Remote())

	srcId, srcDirectoryID, srcLeaf, dstDirectoryID, dstLeaf, err :=
		f.dirCache.DirMove(ctx, srcObject.fs.dirCache, srcObject.fs.root, srcObject.Remote(), f.root, dstRemote)
	if err != nil {
		return nil, err
	}

	if dstDirectoryID != srcDirectoryID {
		err = f._ServerFolderMove(ctx, srcId, dstDirectoryID)
		if err != nil {
			return nil, fs.ErrorCantDirMove
		}
	}

	if dstLeaf != srcLeaf {
		err = f._ServerFolderEdit(ctx, srcId, dstLeaf)
		if err != nil {
			return nil, fs.ErrorCantDirMove
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
	srcLeaf, srcDirId, err := srcObject.fs.dirCache.FindPath(ctx, srcObject.Remote(), false)
	if err != nil {
		return nil, fs.ErrorCantMove
	}

	dstLeaf, dstDirId, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, fs.ErrorCantMove
	}

	if dstDirId != srcDirId {
		err = f._ServerFileMove(ctx, srcObject.itemId, dstDirId)
		if err != nil {
			return nil, fs.ErrorCantMove
		}
	}

	if dstLeaf != srcLeaf {
		err = f._ServerFileRename(ctx, srcObject.itemId, f.opt.Enc.FromStandardName(dstLeaf))
		if err != nil {
			return nil, fs.ErrorCantMove
		}
	}

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

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func (o *Object) DownloadRange(ctx context.Context, id int, url string, start, end int64, options ...fs.OpenOption) (io.ReadCloser, error) {
	var opts rest.Opts
	var res *http.Response
	var err error
	if start == end {
		opts = rest.Opts{
			Method:  "GET",
			RootURL: url,
			Options: options,
		}
		res, err = o.fs._FetchWithRetries(ctx, &opts)
		if err != nil {
			return nil, fmt.Errorf("failed to download %s: %w", url, err)
		}
	} else {
		pacer := o.fs.rxPacers[id % len(o.fs.rxPacers)]
		pacer.Call(func() (bool, error) {
			// Use REST Call api would invalidate multipart downloads and somehow hit the panic:
			// multi-thread copy: failed to write chunk N: wrote X bytes but expected to write Y
			req, _ := http.NewRequest("GET", url, nil)
			req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))
			res, err = o.fs.downloader.Do(req)
			return o.fs.shouldRetry(ctx, res, err)
		})
		if err != nil {
			return nil, err
		}
	}

	return res.Body, nil
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

	if o.fs.opt.MultipartRxConcurrency == 0 {
		return o.DownloadRange(ctx, 0, downloadURL, 0, 0, options...)
	}

	// Check if there is one single range option after FixRangeOption
	start, end := int64(0), int64(0)
	numRangeOptions := 0
	for i := range options {
		if rangeOption, ok := options[i].(*fs.RangeOption); ok {
			numRangeOptions++
			if rangeOption.Start >= 0 && rangeOption.Start < rangeOption.End {
				start = rangeOption.Start
				end = rangeOption.End
			}
		}
	}

	// Don't support no/multiple range options
	if numRangeOptions != 1 {
		return o.DownloadRange(ctx, 0, downloadURL, 0, 0, options...)
	}

	length := end - start + 1
	concurrency := o.fs.opt.MultipartRxConcurrency
	partSize := length / int64(concurrency)
	remainder := length % int64(concurrency)
	if remainder != 0 {
		concurrency++
	}

	// There is no need for multipart download
	if concurrency == 1 {
		return o.DownloadRange(ctx, 0, downloadURL, start, end, options...)
	}

	// Too small size to fit multipart download
	if partSize < minDownloadPartSize {
		return o.DownloadRange(ctx, 0, downloadURL, 0, 0, options...)
	}

	// Multipart download
	wg := &sync.WaitGroup{}
	parts := make([]io.ReadCloser, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		start_ := start + int64(i) * partSize
		end_ := start + int64(i + 1) * partSize - 1

		if i == (concurrency - 1) && remainder > 0 {
			end_ = start_ + remainder - 1
		}

		go func(start, end int64, partLoc *io.ReadCloser, id int) {
			*partLoc, _ = o.DownloadRange(ctx, id, downloadURL, start, end, options...)
			wg.Done()
		}(start_, end_, &parts[i], i)
	}
	wg.Wait()

	successfulParts := make([]io.ReadCloser, 0)
	numSuccessfulParts := 0
	for i := range parts {
		if parts[i] != nil {
			numSuccessfulParts++
			successfulParts = append(successfulParts, parts[i])
		}
	}

	if numSuccessfulParts != concurrency {
		// Ensure downloaded parts being closed to release resources
		for i := range successfulParts {
			successfulParts[i].Close()
		}
		return nil, fmt.Errorf("failed to download %d parts for %s", concurrency - numSuccessfulParts, downloadURL)
	}

	return readcloser.MultiReadCloser(parts...), nil
}

func (o *Object) UploadParts(in *io.Reader, metadata *api.FileUploadSessionRes, inSize int64, partSize int64) error {
	// Calibrate concurrency as needed by the memory constraint
	calibratedConcurrency := o.fs.opt.MultipartTxConcurrency
	estimatedBufferSize := partSize * int64(calibratedConcurrency)
	if estimatedBufferSize > o.fs.opt.MultipartTxMaxBufferSize {
		calibratedConcurrency = int(o.fs.opt.MultipartTxMaxBufferSize / partSize)
	}
	if calibratedConcurrency < 1 {
		calibratedConcurrency = 1
	}

	partCount := int(inSize / partSize)
	if inSize % partSize != 0 {
		partCount++
	}

	if partCount > maxPerUploadParts {
		return fmt.Errorf("too many parts: %d > %d", partCount, maxPerUploadParts)
	}

	obsClient, err := obs.New(
		metadata.Data.Ak,
		metadata.Data.Sk,
		metadata.Data.Server,
		obs.WithSecurityToken(metadata.Data.SToken),
		obs.WithHeaderTimeout(o.fs.opt.MultipartResponseHeaderTimeout),
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

	parts := make([]obs.Part, partCount)
	wg := &sync.WaitGroup{}
	tickets := make(chan struct{}, calibratedConcurrency)
	GetTicket := func() {
		tickets <- struct{}{}
		wg.Add(1)
	}
	ReleaseTicket := func() {
		<- tickets
		wg.Done()
	}
	WaitForAllTcketsDone := func() {
		wg.Wait()
	}
	hasFailures := false
	for i := 0; i < partCount && !hasFailures; i += calibratedConcurrency {
		concurrency := calibratedConcurrency
		if i + concurrency > partCount {
			concurrency = partCount - i
		}
		for j := i; j < i + concurrency && !hasFailures; j++ {
			GetTicket()
			partNumber := j + 1
			partReader := io.LimitReader(*in, partSize)
			partContent, err := io.ReadAll(partReader)
			if err != nil {
				hasFailures = true
				ReleaseTicket()
				parts[partNumber - 1] = obs.Part{ETag: "", PartNumber: -1}
				log.Default().Printf("failed to prepare part %d: %v", partNumber, err)
				break
			}
			body := bytes.NewReader(partContent)

			go func(partNumber int, body *bytes.Reader) {
				defer ReleaseTicket()

				uploadPartInput := &obs.UploadPartInput{}
				uploadPartInput.Bucket = metadata.Data.Bucket
				uploadPartInput.Key = metadata.Data.PoolPath
				uploadPartInput.UploadId = initiateMultipartUploadOutput.UploadId
				uploadPartInput.PartNumber = partNumber

				if o.fs.opt.MultipartTxIntegrity {
					h := md5.New()
					_, err := io.Copy(h, body)
					if err != nil {
						hasFailures = true
						log.Default().Printf("failed to hash part %d: %v", partNumber, err)
						return
					}
					uploadPartInput.ContentMD5 = obs.Base64Encode(h.Sum(nil))
				}

				var uploadPartInputOutput *obs.UploadPartOutput
				pacer := o.fs.txPacers[partNumber % len(o.fs.txPacers)]
				pacer.Call(func() (bool, error) {
					body.Seek(0, io.SeekStart)
					uploadPartInput.Body = body
					uploadPartInputOutput, err = obsClient.UploadPart(uploadPartInput)
					return err != nil, err
				})

				if err == nil {
					parts[partNumber - 1] = obs.Part{ETag: uploadPartInputOutput.ETag, PartNumber: uploadPartInputOutput.PartNumber}
				} else {
					parts[partNumber - 1] = obs.Part{ETag: "", PartNumber: -1}
					log.Default().Printf("failed to upload part %d: %v", partNumber, err)
					hasFailures = true
				}
			}(partNumber, body)
		}
	}
	WaitForAllTcketsDone()

	if hasFailures {
		return fmt.Errorf("failed to upload parts, try reducing rclone --transfers or advanced tx concurrency settings")
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
	first10m := io.LimitReader(in, 10_485_760)
	first10mBytes, err := io.ReadAll(first10m)
	if err != nil {
		return err
	}

	vgroup := fmt.Sprintf("%x", md5.Sum(first10mBytes)) + "_" + strconv.FormatInt(src.Size(), 10)
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/file/get_file_upload_session",
		Options: options,
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
		err = o.UploadParts(&file, &fileUploadSessionRes, src.Size(), o.fs.opt.MultipartTxPartSize)
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

	_, name := splitDirAndName(fullPath)
	pdirToCreateIfNotExists, _ := splitDirAndName(o.Remote())

	dirId, err := o.fs.dirCache.FindDir(ctx, pdirToCreateIfNotExists, true)
	if err != nil {
		return err
	}
	pid, _ := strconv.Atoi(dirId)

	filename := o.fs.opt.Enc.FromStandardName(name)
	opts = rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/file/create_item",
		Options: options,
		Parameters: url.Values{
			"diyName":    []string{filename},
			"filename":   []string{filename},
			"pid":        []string{dirId},
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

	// Try a few times to read the object after upload for eventual consistency
	const maxTries = 10
	var sleepTime = 100 * time.Millisecond
	var entity api.Entity
	for try := 1; try <= maxTries; try++ {
		entity, err = getObject(o.fs, ctx, name, pid, o.fs.opt.Token)
		if err == nil {
			break
		}
		if err != fs.ErrorObjectNotFound {
			return fmt.Errorf("Update failed to read object: %w", err)
		}
		fs.Debugf(o, "Trying to read object after upload: try again in %v (%d/%d)", sleepTime, try, maxTries)
		time.Sleep(sleepTime)
		sleepTime *= 2
	}
	if err != nil {
		return err
	}
	o.set(&entity)
	return nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	size := src.Size()
	if size == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}

	// remove the file if it exists
	if o.itemId != "" {
		fs.Debugf(o, "Update: removing old file")
		err = o.Remove(ctx)
		if err != nil {
			fs.Errorf(o, "Update: failed to remove existing file: %v", err)
		}
		o.itemId = ""
	} else {
		tmpObject, err := o.fs.NewObject(ctx, o.Remote())
		if err == nil {
			fs.Debugf(o, "Update: removing old file")
			err = tmpObject.Remove(ctx)
			if err != nil {
				fs.Errorf(o, "Update: failed to remove existing file: %v", err)
			}
		}
	}

	if o.fs.opt.MultipartTxConcurrency > 0 && src.Size() >= minPartSize {
		return o._MultipartUpload(ctx, in, src, options...)
	}

	first10m := io.LimitReader(in, 10_485_760)
	first10mBytes, err := io.ReadAll(first10m)
	if err != nil {
		return fmt.Errorf("Update err in reading file: %w", err)
	}

	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/get_upload_url",
		Options: options,
		Parameters: url.Values{
			"fileMd5ofPre10m": []string{fmt.Sprintf("%x", md5.Sum(first10mBytes))},
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
			Options: options,
			Body:    file,
			ContentLength: &size,
		}

		res, err := o.fs._FetchWithRetries(ctx, &opts)
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

	name, dirId, err := o.fs.dirCache.FindPath(ctx, o.Remote(), false)
	if err != nil {
		return err
	}
	pid, _ := strconv.Atoi(dirId)

	opts = rest.Opts{
		Method:  "GET",
		RootURL: "https://www.linkbox.to/api/open/folder_upload_file",
		Options: options,
		Parameters: url.Values{
			"fileMd5ofPre10m": []string{fmt.Sprintf("%x", md5.Sum(first10mBytes))},
			"fileSize":        []string{strconv.FormatInt(src.Size(), 10)},
			"pid":             []string{dirId},
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

	// Try a few times to read the object after upload for eventual consistency
	const maxTries = 10
	var sleepTime = 100 * time.Millisecond
	var entity api.Entity
	for try := 1; try <= maxTries; try++ {
		entity, err = getObject(o.fs, ctx, name, pid, o.fs.opt.Token)
		if err == nil {
			break
		}
		if err != fs.ErrorObjectNotFound {
			return fmt.Errorf("Update failed to read object: %w", err)
		}
		fs.Debugf(o, "Trying to read object after upload: try again in %v (%d/%d)", sleepTime, try, maxTries)
		time.Sleep(sleepTime)
		sleepTime *= 2
	}
	if err != nil {
		return err
	}
	o.set(&entity)
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
	dir, _ := splitDirAndName(src.Remote())
	err := f.Mkdir(ctx, dir)
	if err != nil {
		return nil, err
	}
	err = o.Update(ctx, in, src, options...)
	return o, err
}

func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = &Fs{}
	_ fs.DirMover        = &Fs{}
	_ fs.Mover           = &Fs{}
	_ fs.Purger          = &Fs{}
	_ fs.Abouter         = &Fs{}
	_ fs.DirCacheFlusher = &Fs{}
	_ fs.Object          = &Object{}
	_ fs.MimeTyper       = &Object{}
)
