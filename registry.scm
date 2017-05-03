;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'registry)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger})
(define %used_modules 'ezrecords)

(module-export! '{use-registry set-registry!
		  register registry/ref
		  registry/save!})

;; Not yet used
(define default-registry #f)

(define (registry->string r)
  (stringout "#<REGISTRY " (registry-slotid r) " " (registry-spec r) ">"))

(define-init registries (make-hashtable))

(defrecord (registry OPAQUE `(stringfn . registry->string))
  slotid spec server pool index (idstream #f) (bloom #f)
  (cache (make-hashtable))
  (lock (make-condvar))
  (opts #[]))

(define (register slotid value (inits #f) (registry))
  (default! registry (try (get registries slotid) #f))
  (cond ((and registry (not inits))
	 (try (get (registry-cache registry) value)
	      (registry/get registry slotid value #t)))
	(registry
	 (let ((result (try (get (registry-cache registry) value)
			    (registry/get registry slotid value #t))))
	   (do-choices (slotid (getkeys inits))
	     (unless (test result slotid)
	       (store! result slotid (get inits slotid))))))
	(else (irritant slotid "No Registry"
		"No registry exists for the slot " (write slotid)))))

(define (registry/ref registry slotid value (create #f))
  (default! registry (try (get registries slotid) #f))
  (unless registry (set! registry (try (get registries slotid) #f)))
  (if registry
      (try (get (registry-cache registry) value)
	   (registry/get registry slotid value #f))
      (irritant slotid "No Registry"
	"No registry exists for the slot " (write slotid))))

(define (registry/save! (r #f))
  (when (and (or (symbol? r) (oid? r)) (test registries r))
    (set! r (get registries r)))
  (cond ((not r)
	 (let ((threads (for-choices (r (get registries (getkeys registries)))
			  (threadcall registry/save! r))))
	   (threadjoin threads)))
	((and (registry? r) (not (registry-server r)))
	 (let ((threads
		(choice (threadcall commit ({registry-pool registry-index} r))
			(tryif  (registry-idstream r)
			  (threadcall flush-output (registry-idstream r))))))
	   (threadjoin threads)))
	((registry? r)
	 (logwarn |RemoteRegistry| "Can't save a remote registry"))
	((or (symbol? r) (oid? r))
	 (logwarn |NoRegistry| "No registry for slotid " r " to save"))
	(else (irritant r |NotARegistry| registry/save!))))

;;; The meat of it

(define (registry/get registry slotid value (create #f) (server))
  (default! server (registry-server registry))
  (with-lock (registry-lock registry)
    (try (get (registry-cache registry) value)
	 (let* ((bloom (and (not server) (registry-bloom registry)))
		(existing (if server
			      (find-frames (registry-index registry)
				slotid value)
			      (tryif (or (not bloom)
					 (bloom/check bloom value))
				(find-frames (registry-index registry)
				  slotid value))))
		(result (try existing
			     (tryif create
			       (if server (dtcall server 'register slotid value)
				   (frame-create (registry-pool registry)
				     '%id (list slotid value)
				     slotid value))))))
	   (when (exists? result)
	     (when (and (fail? existing) (not (registry-server registry)))
	       (index-frame (registry-index registry) result slotid value))
	     (when bloom
	       (dtype->file+ value (registry-idstream registry))
	       (bloom/add! bloom value))
	     (store! (registry-cache registry) value result))
	   result))))

;;; Registering registries

(defslambda (register-registry slotid spec server pool index (replace #f))
  (when replace (drop! registries slotid))
  (try (get registries slotid)
       (let* ((ixsource (if (string? index) index
			    (index-source index)))
	      (idpath (and (not server) (string? ixsource) 
			   (not (exists position {#\: #\@} ixsource))
			   (glom ixsource ".ids")))
	      (idstream (and idpath (extend-dtype-file idpath)))
	      (bloom (and idpath (get-bloom idpath)))
	      (registry (cons-registry slotid spec server pool index idstream bloom)))
	 (store! registries slotid registry)
	 registry)))

(define (use-registry slotid spec (second-spec #f) (third-spec #f))
  (try (get registries slotid)
       (let* ((server (and (string? spec)
			   (exists position {#\@ #\:} spec)
			   (open-dtserver spec)))
	      (pool (if (and server second-spec) 
			(use-pool second-spec)
			(use-pool spec)))
	      (index (open-index (or third-spec second-spec spec))))
	 (register-registry slotid spec server pool index))))
(define (set-registry! slotid spec (second-spec #f) (third-spec #f))
  (if (and (test registries slotid) (string? spec)
	   (exists position {#\@ #\:} spec)
	   (registry-server (get registries slotid))
	   (equal? (dtserver-id (get registries slotid)) spec))
      (get registries slotid)
      (let* ((server (and (string? spec)
			  (exists position {#\@ #\:} spec)
			  (open-dtserver spec)))
	     (pool (if (and server second-spec) 
		       (use-pool second-spec)
		       (use-pool spec)))
	     (index (open-index (or second-spec spec))))
	(if (test registries slotid)
	    (let ((cur (get registries slotid)))
	      (if (and (equal? server (registry-server cur))
		       (equal? pool (registry-pool cur))
		       (equal? index (registry-index cur)))
		  cur
		  (register-registry spec slotid server pool index #t)))
	    (register-registry slotid spec server pool index #t)))))

;;; Getting the ids for a bloom filter

(define (get-bloom path (error #f))
  (if (and (string? path) (file-exists? path))
       (let* ((ids (file->dtypes path))
	      (bloom (make-bloom-filter (* 8 (max (choice-size ids) 4000000)) 
					(or error 0.0001))))
	 (bloom/add! bloom ids)
	 bloom)
       (make-bloom-filter 8000000 (or error 0.0001))))



