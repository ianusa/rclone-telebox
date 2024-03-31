Rclone-telebox is an enhanced version of [rclone](https://github.com/rclone/rclone) extending and enhancing capabilitis of Telebox (was Linkbox) backend.

## The Feature Highlight ##
* Support Aborter which provides the possibility to union multiple cloud disks
* Support DirMover which moves/renames directories on the server
* Support Mover which moves/renames files on the server
* Boost upload speed by multipart uploads
* Boost download speed by multi-stream downloads
* Advanced options to configure upload/download concurrencies
* Advanced option to validate integrity of uploaded files
* Advanced option to change User-Agent

## Test ##
~~~bash
=== RUN   TestIntegration
    fstests.go:430: Using remote "TestLinkbox:"
=== RUN   TestIntegration/FsCheckWrap
    fstests.go:471: Not a wrapping Fs
=== RUN   TestIntegration/FsCommand
    fstests.go:499: No commands in this remote
=== RUN   TestIntegration/FsRmdirNotFound
=== RUN   TestIntegration/FsString
=== RUN   TestIntegration/FsName
=== RUN   TestIntegration/FsRoot
=== RUN   TestIntegration/FsRmdirEmpty
=== RUN   TestIntegration/FsMkdir
=== RUN   TestIntegration/FsMkdir/FsMkdirRmdirSubdir
=== RUN   TestIntegration/FsMkdir/FsListEmpty
=== RUN   TestIntegration/FsMkdir/FsListDirEmpty
=== RUN   TestIntegration/FsMkdir/FsListRDirEmpty
    fstests.go:391: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsListDirNotFound
2024/03/30 21:20:24 ERROR : does not exist: error listing: directory not found
=== RUN   TestIntegration/FsMkdir/FsListRDirNotFound
    fstests.go:391: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsEncoding
=== RUN   TestIntegration/FsMkdir/FsEncoding/control_chars
    fstests.go:696: testing "␀␁␂␃␄␅␆␇␈␉␊␋␌␍␎␏␐␑␒␓␔␕␖␗␘␙␚␛␜␝␞␟␡"
=== RUN   TestIntegration/FsMkdir/FsEncoding/dot
    fstests.go:696: testing "．"
=== RUN   TestIntegration/FsMkdir/FsEncoding/dot_dot
    fstests.go:696: testing "．．"
=== RUN   TestIntegration/FsMkdir/FsEncoding/punctuation
    fstests.go:696: testing "!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_space
    fstests.go:696: testing " leading space"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_tilde
    fstests.go:696: testing "~leading tilde"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_CR
    fstests.go:696: testing "␍leading CR"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_LF
    fstests.go:696: testing "␊leading LF"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_HT
    fstests.go:696: testing "␉leading HT"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_VT
    fstests.go:696: testing "␋leading VT"
=== RUN   TestIntegration/FsMkdir/FsEncoding/leading_dot
    fstests.go:696: testing ".leading dot"
=== RUN   TestIntegration/FsMkdir/FsEncoding/trailing_space
    fstests.go:696: testing "trailing space "
=== RUN   TestIntegration/FsMkdir/FsEncoding/trailing_CR
    fstests.go:696: testing "trailing CR␍"
=== RUN   TestIntegration/FsMkdir/FsEncoding/trailing_LF
    fstests.go:696: testing "trailing LF␊"
=== RUN   TestIntegration/FsMkdir/FsEncoding/trailing_HT
    fstests.go:696: testing "trailing HT␉"
=== RUN   TestIntegration/FsMkdir/FsEncoding/trailing_VT
    fstests.go:696: testing "trailing VT␋"
=== RUN   TestIntegration/FsMkdir/FsEncoding/trailing_dot
    fstests.go:696: testing "trailing dot."
=== RUN   TestIntegration/FsMkdir/FsEncoding/invalid_UTF-8
    fstests.go:696: testing "invalid utf-8\xfe"
=== RUN   TestIntegration/FsMkdir/FsEncoding/URL_encoding
    fstests.go:696: testing "test%46.txt"
=== RUN   TestIntegration/FsMkdir/FsNewObjectNotFound
=== RUN   TestIntegration/FsMkdir/FsPutError
=== RUN   TestIntegration/FsMkdir/FsPutZeroLength
    fstests.go:236: Can't upload zero length files
=== RUN   TestIntegration/FsMkdir/FsOpenWriterAt
    fstests.go:769: FS has no OpenWriterAt interface
=== RUN   TestIntegration/FsMkdir/FsOpenChunkWriter
    fstests.go:802: FS has no OpenChunkWriter interface
=== RUN   TestIntegration/FsMkdir/FsChangeNotify
    fstests.go:851: FS has no ChangeNotify interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListDirFile2
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListRDirFile2
    fstests.go:391: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListR
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListRSubdir
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListDirRoot
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListRDirRoot
    fstests.go:391: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListSubdir
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListRSubdir#01
    fstests.go:391: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListLevel2
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListRLevel2
    fstests.go:391: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListFile1
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsNewObject
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsNewObjectCaseInsensitive
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsNewObjectCaseInsensitive/Dir
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsListFile1and2
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsNewObjectDir
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsPurge
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsPurgeRoot
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsCopy
    fstests.go:1215: FS has no Copier interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsMove
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsMove/Metadata
    fstests.go:1352: Skipping test as can't write metadata
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsDirMove
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsRmdirFull
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsPrecision
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectString
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectFs
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectRemote
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectHashes
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectModTime
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectMimeType
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectMetadata
    fstests.go:1577: Metadata method not supported
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectSetModTime
    fstests.go:1644: can't set modified time
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectSize
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectOpen
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectOpenSeek
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectOpenRange
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectPartialRead
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectUpdate
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectStorable
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsIsFile
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsIsFile/FsRoot
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsIsFileNotFound
2024/03/30 21:25:06 ERROR : : error listing: directory not found
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FromRoot
    fstests.go:1804: Opening root remote "TestLinkbox:" path "rclone-test-bedusix5kikerov5jehofig0" from "TestLinkbox:rclone-test-bedusix5kikerov5jehofig0"
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FromRoot/List
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FromRoot/ListEntries
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FromRoot/ListR
    fstests.go:1838: FS has no ListR interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FromRoot/Put
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FromRoot/Put/Remove
=== RUN   TestIntegration/FsMkdir/FsPutFiles/PublicLink
    fstests.go:1897: FS has no PublicLinker interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/SetTier
    fstests.go:404: FS has no SetTier & GetTier interfaces
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectCheckWrap
    fstests.go:2002: Not a wrapping Fs
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectRemove
=== RUN   TestIntegration/FsMkdir/FsPutFiles/ObjectAbout
=== RUN   TestIntegration/FsMkdir/FsPutFiles/FsPutStream
    fstests.go:2054: FS has no PutStream interface
=== RUN   TestIntegration/FsMkdir/FsPutFiles/Internal
    fstests.go:2098: *linkbox.Fs does not implement InternalTester
=== RUN   TestIntegration/FsMkdir/FsPutChunked
    fstests.go:2119: *linkbox.Fs does not implement SetUploadChunkSizer
=== RUN   TestIntegration/FsMkdir/FsCopyChunked
    fstests.go:2237: FS has no Copier interface
=== RUN   TestIntegration/FsMkdir/FsUploadUnknownSize
=== RUN   TestIntegration/FsMkdir/FsUploadUnknownSize/FsPutUnknownSize
=== RUN   TestIntegration/FsMkdir/FsUploadUnknownSize/FsUpdateUnknownSize
=== RUN   TestIntegration/FsMkdir/FsRootCollapse
=== RUN   TestIntegration/FsMkdir/FsDirSetModTime
    fstests.go:2386: FS has no DirSetModTime interface
=== RUN   TestIntegration/FsMkdir/FsMkdirMetadata
    fstests.go:2429: FS has no MkdirMetadata interface
=== RUN   TestIntegration/FsMkdir/FsDirectory
    fstests.go:2486: FS has no Directory methods and doesn't Wrap
2024/03/30 21:25:30 ERROR : : error listing: directory not found
=== RUN   TestIntegration/FsShutdown
    fstests.go:2592: Shutdown method not supported
--- PASS: TestIntegration (319.85s)
    --- SKIP: TestIntegration/FsCheckWrap (0.00s)
    --- SKIP: TestIntegration/FsCommand (0.00s)
    --- PASS: TestIntegration/FsRmdirNotFound (0.21s)
    --- PASS: TestIntegration/FsString (0.00s)
    --- PASS: TestIntegration/FsName (0.00s)
    --- PASS: TestIntegration/FsRoot (0.00s)
    --- PASS: TestIntegration/FsRmdirEmpty (1.51s)
    --- PASS: TestIntegration/FsMkdir (315.52s)
        --- PASS: TestIntegration/FsMkdir/FsMkdirRmdirSubdir (7.35s)
        --- PASS: TestIntegration/FsMkdir/FsListEmpty (0.21s)
        --- PASS: TestIntegration/FsMkdir/FsListDirEmpty (0.22s)
        --- SKIP: TestIntegration/FsMkdir/FsListRDirEmpty (0.00s)
        --- PASS: TestIntegration/FsMkdir/FsListDirNotFound (0.22s)
        --- SKIP: TestIntegration/FsMkdir/FsListRDirNotFound (0.00s)
        --- PASS: TestIntegration/FsMkdir/FsEncoding (181.61s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/control_chars (9.15s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/dot (11.93s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/dot_dot (9.15s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/punctuation (9.10s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_space (9.13s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_tilde (9.16s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_CR (9.59s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_LF (9.32s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_HT (9.21s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_VT (9.15s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/leading_dot (12.75s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/trailing_space (9.45s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/trailing_CR (9.05s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/trailing_LF (9.17s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/trailing_HT (9.20s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/trailing_VT (9.34s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/trailing_dot (9.00s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/invalid_UTF-8 (9.39s)
            --- PASS: TestIntegration/FsMkdir/FsEncoding/URL_encoding (9.14s)
        --- PASS: TestIntegration/FsMkdir/FsNewObjectNotFound (0.45s)
        --- PASS: TestIntegration/FsMkdir/FsPutError (6.47s)
        --- SKIP: TestIntegration/FsMkdir/FsPutZeroLength (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsOpenWriterAt (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsOpenChunkWriter (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsChangeNotify (0.00s)
        --- PASS: TestIntegration/FsMkdir/FsPutFiles (105.95s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListDirFile2 (1.03s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsListRDirFile2 (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListR (1.02s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListRSubdir (0.83s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListDirRoot (0.44s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsListRDirRoot (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListSubdir (0.40s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsListRSubdir#01 (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListLevel2 (0.40s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsListRLevel2 (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListFile1 (1.03s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsNewObject (0.22s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsNewObjectCaseInsensitive (1.28s)
                --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsNewObjectCaseInsensitive/Dir (1.05s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsListFile1and2 (1.01s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsNewObjectDir (0.23s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsPurge (8.97s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsPurgeRoot (10.88s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsCopy (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsMove (20.93s)
                --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsMove/Metadata (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsDirMove (15.75s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsRmdirFull (0.22s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsPrecision (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectString (0.20s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectFs (0.20s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectRemote (0.20s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectHashes (0.20s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectModTime (0.20s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectMimeType (0.20s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/ObjectMetadata (0.21s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectSetModTime (0.20s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectSize (0.24s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectOpen (0.96s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectOpenSeek (0.26s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectOpenRange (1.03s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectPartialRead (0.44s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectUpdate (6.48s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectStorable (0.22s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsIsFile (2.89s)
                --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsIsFile/FsRoot (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FsIsFileNotFound (1.23s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/FromRoot (9.14s)
                --- PASS: TestIntegration/FsMkdir/FsPutFiles/FromRoot/List (1.25s)
                --- PASS: TestIntegration/FsMkdir/FsPutFiles/FromRoot/ListEntries (0.20s)
                --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FromRoot/ListR (0.00s)
                --- PASS: TestIntegration/FsMkdir/FsPutFiles/FromRoot/Put (7.49s)
                    --- PASS: TestIntegration/FsMkdir/FsPutFiles/FromRoot/Put/Remove (2.76s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/PublicLink (0.00s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/SetTier (0.00s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/ObjectCheckWrap (0.00s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectRemove (3.05s)
            --- PASS: TestIntegration/FsMkdir/FsPutFiles/ObjectAbout (0.20s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/FsPutStream (0.00s)
            --- SKIP: TestIntegration/FsMkdir/FsPutFiles/Internal (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsPutChunked (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsCopyChunked (0.00s)
        --- PASS: TestIntegration/FsMkdir/FsUploadUnknownSize (8.30s)
            --- PASS: TestIntegration/FsMkdir/FsUploadUnknownSize/FsPutUnknownSize (1.30s)
            --- PASS: TestIntegration/FsMkdir/FsUploadUnknownSize/FsUpdateUnknownSize (7.00s)
        --- PASS: TestIntegration/FsMkdir/FsRootCollapse (1.25s)
        --- SKIP: TestIntegration/FsMkdir/FsDirSetModTime (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsMkdirMetadata (0.00s)
        --- SKIP: TestIntegration/FsMkdir/FsDirectory (0.00s)
    --- SKIP: TestIntegration/FsShutdown (0.00s)
PASS
ok      github.com/rclone/rclone/backend/linkbox        319.931s
~~~
