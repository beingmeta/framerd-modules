;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'zipfs)

;;; Virtual file system implemented on top of zipfiles and hashtables

(use-module '{mimetable ezrecords texttools 
	      logger gpath ziptools})
(define %used_modules '{ezrecords mimetable})

(module-export! '{zipfs? zipfs/open zipfs/make zipfs/save!
		  zipfs/filename zipfs/string
		  zipfs/get zipfs/get+ zipfs/info 
		  zipfs/list zipfs/list+
		  zipfs/commit!})

(defrecord (zipfs OPAQUE)
  files (zip #f) (opts #f) (source #f)
  (sync #f) (bemeta #f))

(define (zipfs/open (source #f) (opts #f) (create))
  (when (and (not opts) (table? source) (not (gpath? source)))
    (set! opts source)
    (set! source (getopt opts 'source #f)))
  (default! create (getopt opts 'create #f))
  (when source (set! source (->gpath source)))
  (when (and (not create) (or (not source) (not (gp/exists? source))))
    (if source 
	(irritant source |NoSuchFile|)
	(error |NoSourceSpecified|
	    "Use zipfs/make to create an anonymous ZIPFS")))
  (let* ((zipfile (get-zipfile source opts))
	 (has-metadata (zip/exists? zipfile ".zipfs/bemeta"))
	 (zipfs (cons-zipfs (make-hashtable) zipfile opts source 
			    (getopt opts 'sync #f)
			    (getopt opts 'bemeta has-metadata))))
    (when (and (not has-metadata) (getopt opts 'bemeta))
      (zip/add! zipfile ".zipfs/bemeta" (get (gmtimestamp) 'iso)))
    zipfs))
(define (zipfs/make (source #f) (opts #f) (create #t))
  (when (and (not opts) (table? source) (not (gpath? source)))
    (set! opts source)
    (set! source (getopt opts 'source #f)))
  (when source (set! source (->gpath source)))
  (let* ((zipfile (get-zipfile source opts))
	 (has-metadata (zip/exists? zipfile ".zipfs/bemeta"))
	 (zipfs (cons-zipfs (make-hashtable) zipfile opts source 
			    (getopt opts 'sync #f)
			    (getopt opts 'bemeta has-metadata))))
    (when (and (not has-metadata) (getopt opts 'bemeta))
      (zip/add! zipfile ".zipfs/bemeta" (get (gmtimestamp) 'iso)))
    zipfs))

(define (zipfs/string zipfs path)
  (stringout "zipfs:" path "(" (gpath->string (zipfs-source zipfs)) ")"))

(define (zipfs->string zipfs)
  (stringout "#<ZIPFS " 
    (when (zipfs-source zipfs) (write (gpath->string (zipfs-source zipfs))))
    " "
    (write (zip/filename (zipfs-zip zipfs)))
    ">"))
(compound-set-stringfn! 'ZIPFS zipfs->string)

(define (zipfs/filename zipfs)
  (zip/filename (zipfs-zip zipfs)))

(define (get-zipfile source opts (copy))
  (default! copy (getopt opts 'copy #f))
  (cond ((zipfile? source) source)
	((and (gp/exists? source) copy)
	 (irritant source |ZipFSConflict|
		   " already exists, can't copy from " copy))
	((gp/exists? source) (zip/open source))
	(else (let* ((tmpdir (getopt opts 'tmpdir 
				     (tempdir (getopt opts 'tmplate)
					      (getopt opts 'keeptemp))))
		     (name (getopt opts 'name
				   (if source (gp/basename source)
				       (if copy (gp/basename copy)
					   "zipfs.zip"))))
		     (path (mkpath tmpdir name))
		     (zip #f))
		(cond ((and source (gp/exists? source))
		       (gp/copy! source path)
		       (set! zip (zip/open path opts)))
		      ((and copy (gp/exists? copy))
		       (gp/copy! copy path)
		       (set! zip (zip/open path opts))))
		(or zip (zip/make path))))))

(define (zipfs/save! zipfs path data (type) (metadata #f))
  (default! type 
    (getopt metadata 'content-type 
	    (path->mimetype path (if (packet? data) "application" "text"))))
  (when (and (not metadata) (table? type))
    (set! metadata type)
    (set! type (path->mimetype path (if (packet? data) "application" "text"))))
  (if metadata
      (set! metadata (deep-copy metadata))
      (set! metadata (frame-create #f)))
  (store! metadata 'ctype type)
  (store! metadata 'modified (gmtimestamp))
  (when (zipfs-bemeta zipfs)
    (zip/add! (zipfs-zip zipfs)
	      (glom ".zipfs/" path)
	      (dtype->packet metadata)))
  (store! metadata 'content data)
  (store! (zipfs-files zipfs) path metadata)
  (zip/add! (zipfs-zip zipfs) path data)
  (when (zipfs-sync zipfs) (zip/close (zipfs-zip zipfs))))

(define (zip-info zip path opts)
  (tryif (zip/exists? zip path)
    (let* ((ctype (path->mimetype path #f))
	   (encoding (path->encoding path))
	   (istext (and ctype (mimetype/text? ctype) (not encoding)))
	   (charset (and istext (ctype->charset ctype)))
	   (entry (frame-create #f
		    'ctype (tryif ctype ctype)
		    'charset (if (string? charset) charset (if charset "utf-8" {}))
		    'modified (tryif (bound? zip/modtime)
				(zip/modtime zip path)))))
      entry)))

(define (cached-content files zip path content opts)
  (let* ((ctype (getopt opts 'ctype 
			(path->mimetype path #f (getopt opts 'typemap))))
	 (encoding (getopt opts 'encoding (path->encoding path)))
	 (istext (and ctype (mimetype/text? ctype) (not encoding)))
	 (charset (and istext (ctype->charset ctype)))
	 (content (zip/get zip path))
	 (entry (frame-create #f
		  'content
		  (cond ((and (string? content) istext) content)
			((and (packet? content) istext)
			 (packet->string content charset))
			((packet? content) content)
			((string? content) (string->packet content charset))
			(else (irritant content "Not a string or packet")))
		  'ctype (tryif ctype ctype)
		  'charset (if (string? charset) charset (if charset "utf-8" {}))
		  'modified (tryif (bound? zip/modtime) (zip/modtime zip realpath))
		  'etag (packet->base16 (md5 content)))))
    (store! files path entry)
    entry))

(define (zipfs/get zipfs path (opts #f) (files) (zip))
  (set! files (zipfs-files zipfs))
  (set! zip (zipfs-zip zipfs))
  (try (get (get files path) 'content)
       (let ((content (zip/get (zipfs-zip zipfs) path)))
	 (if content
	     (get (cached-content files zip path content opts) 'content)
	     (fail)))))
(define (zipfs/get+ zipfs path (opts #f) (files) (zip))
  (set! files (zipfs-files zipfs))
  (set! zip (zipfs-zip zipfs))
  (try (get files path)
       (let ((content (zip/get (zipfs-zip zipfs) path)))
	 (if content
	     (cached-content files zip path content opts)
	     (fail)))))

(define (zipfs/info zipfs path)
  (zip-info (zipfs-zip zipfs) path #f))

(define (zipfs/list zipfs (match #f))
  (let* ((paths (pickstrings (zip/getfiles (zipfs-zip zipfs))))
	 (matching (if match
		       (filter-choices (path paths)
			 (textsearch (qc match) path))
		       paths)))
    (for-choices (path matching)
      (cons zipfs (if (has-prefix path "/") (slice  path 1) path)))))
(define (zipfs/list+ zipfs (match #f))
  (let* ((paths (pickstrings (zip/getfiles (zipfs-zip zipfs))))
	 (matching (if match
		       (filter-choices (path paths)
			 (textsearch (qc match) path))
		       paths))
	 (files (zipfs-files zipfs))
	 (zip (zipfs-zip zipfs)))
    (for-choices (path matching)
      (modify-frame (zip-info zip path #f)
	'path (if (has-prefix path "/") (slice  path 1) path)
	'gpath (gp/mkpath zipfs path)
	'gpathstring (zipfs/string zipfs path)
	'ctype (get (get (zipfs-files zipfs) path) 'ctype)
	'modified (get (get (zipfs-files zipfs) path) 'ctype)))))

(define (zipfs/commit! zipfs)
  (if (zipfs-source zipfs)
      (begin (zip/close! (zipfs-zip zipfs))
	(unless (equal? (zipfs-source zipfs)
			(zip/filename (zipfs-zip zipfs)))
	  (logwarn |CopyingZIPFS|
	    "To " (zipfs-source zipfs)
	    " from "(zip/filename (zipfs-zip zipfs)))
	  (gp/save! (zipfs-source zipfs)
	      (dtype->packet (zipfs-files zipfs)))))
      (error "This ZIPFS doesn't have a source" zipfs)))
