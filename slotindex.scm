;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2017 beingmeta, inc. All rights reserved

(in-module 'slotindex)

(use-module '{logger varconfig})

(module-export! '{slotindex/make slotindex/setup 
		  slotindex/init slotindex/add!
		  slotindex/branch slotindex/merge!
		  slot->index slot/index!
		  slotindex/save!})

(define %loglevel %notice%)

;;; This provides a very simple form of compound index

(define-init rootdir #f)
(config! 'SLOTINDEX:ROOT rootdir)

(define-init default-opts
  #[size 1000000
    flags (DTYPEV2)
    config #[]])
(config! 'SLOTINDEX:OPTS '())

(defambda (slotindex/make dir (opts (cons #[] default-opts))
			  . slots)
  (unless (pair? opts) 
    (set! opts (cons opts default-opts)))

  (let ((prefix #f))
    (unless (position #\/ dir) (set! dir (mkpath rootdir dir)))
    (when (and (not (file-directory? dir))
	       (has-prefix dir "/")
	       (file-directory? (dirname dir)))
      (set! prefix (basename dir))
      (set! dir (dirname dir)))
    (let* ((base (frame-create #f  
		   'slotindex 'slotindex 
		   'directory dir 'prefix prefix
		   'pools (getopt opts 'pools {})
		   'custom #[]
		   'opts opts)))
      (do-choices (slot slots)
	(if (or (symbol? slot) (oid? slot))
	    (slotindex/setup base slot)
	    (if (and (table? slot) (test slot 'slot))
		(begin (store! (get base 'custom) 
			       (get slot 'slot)
			       slot)
		  (slotindex/setup base slot))
		(logwarn |SlotIndex/BadSlotSpec|
		  "Couldn't handle " slot))
	    (slotindex/setup base slot )))
      base)))

(define (get-baseoids pool)
  (let* ((oidbucket-size (config 'OIDBUCKETSIZE (* 1024 1024)))
	 (base (pool-base pool))
	 (capacity (pool-capacity pool))
	 (n-buckets (quotient capacity oidbucket-size))
	 (baseoids {}))
    (dotimes (i n-buckets)
      (set+! baseoids (oid-plus base (* i oidbucket-size))))
    baseoids))

(defambda (slotindex/init index)
  (if (test index 'slots)
      (let* ((copy (frame-create #f 
		     'slotindex 'slotindex
		     'directory (get index 'directory)
		     'prefix (get index 'prefix)
		     'opts (get index 'opts)
		     'custom (get index 'custom))))
	(do-choices (slot (get index 'slots))
	  (slotindex/setup copy slot))
	copy)
      (irritant index |NotSlotIndex|)))

(define (poolref spec dir)
  (if (pool? spec) spec
      (if (or (position #\@ spec) (position #\: spec)
	      (has-prefix spec "/"))
	  (use-pool spec)
	  (use-pool (mkpath dir spec)))))

(defslambda (slotindex/setup base slot)
  (try (pick (get base slot) index?)
       (pick (get base slot) hashtable?)
       (let* ((dir (try (get base 'directory) rootdir))
	      (prefix (try (get base 'prefix) "slix_"))
	      (indexopts (try (get base 'opts) (cons #[] default-opts)))
	      (custom (try (pick (get base slot) table?)
			   (get (getopt indexopts 'custom {}) slot)
			   (get (getopt default-opts 'custom {}) slot)
			   #[]))
	      (opts (cons custom indexopts))
	      (default-name (glom prefix (downcase slot) ".index"))
	      (basefile (getopt opts 'basename default-name))
	      (pools (poolref (choice (get base 'pools)
				      (getopt opts 'pools {})) dir))
	      (path (try (pickstrings (get base slot))
			 (getopt opts 'fullpath (mkpath dir basefile))))
	      (index #f))
	 (when (and (getopt opts 'initialize 
			    (config:boolean (config 'slotindex:initialize #f))) 
		    (file-exists? path))
	   (remove-file path))
	 (when (file-exists? path)
	   (lognotice |UsingIndex| "Using the index at " path " for " slot))
	 (if (file-exists? path)
	     (set! index (open-index path))
	     (let ((opts `#[type ,(getopt opts 'indextype 'hashindex)
			    slots ,(getopt opts 'size 1000000)
			    offtype ,(getopt opts 'offtype 'b40)
			    slotids #(,slot)])
		   (baseoids {(getopt opts 'baseoids {})
			      (get-baseoids pools)}))
	       (lognotice |NewIndex| "Creating new index for " slot " at " path)
	       (when (exists? baseoids) (store! opts 'baseoids baseoids))
	       (set! index (make-index path opts))))
	 (store! base slot index)
	 (add! base 'slots slot)
	 index)))

(define (slotindex/add! index slot (opts #f))
  (cond ((test index 'slots slot))
	((index? opts)
	 (add! index 'slots slot)
	 (store! index slot index))
	(opts
	 (store! (get index 'custom) slot opts)
	 (slotindex/setup index slot))
	(else (slotindex/setup index slot))))

(define (slot->index meta-index slot)
  (try (get meta-index slot)
       (if (get meta-index 'root)
	   (setup-branch-index meta-index slot)
	   (slotindex/setup meta-index slot))))

(defambda (slot/index! index frames slots (values))
  (if (index? index)
      (if (bound? values)
	  (index-frame index frames slots values)
	  (index-frame index frames slots))
      (if (bound? values)
	  (do-choices (slot slots)
	    (index-frame (slot->index index slot) frames slot values))
	  (do-choices (slot slots)
	    (index-frame (slot->index index slot) frames slot)))))

(define (slotindex/save! index)
  (cond ((index? index)
	 (logwarn |IndexSave| "Saving index as slotindex: " index)
	 index)
	((test index 'slotindex 'slotindex)
	 (let* ((slots (get index 'slots))
		(indexes (get index (get index 'slots)))
		(tosave (pick indexes modified?))
		(started (elapsed-time)))
	   (when (fail? tosave)
	     (logwarn |IndexSave| 
	       "No modified indexes for slots: " 
	       (do-choices (slot slots i) 
		 (printout (if (> i 0) ", ") slot))))
	   (when (exists? tosave)
	     (logwarn |IndexSave| "Saving " 
		      (choice-size tosave) "/" (choice-size indexes) 
		      " indexes for slots: "
		      (do-choices (slot (pick slots index tosave) i)
			(printout (if (> i 0) ", ") slot)))
	     (let ((threads (thread/call commit tosave)))
	       (logwarn |IndexSave| "Waiting for " 
			(choice-size threads) " threads")
	       (thread/join threads)
	       (logwarn |IndexSave| 
		 "Saved " (choice-size tosave) " indexes in "
		 (secs->string (elapsed-time started)))))
	   (let ((export
		  (frame-create #f
		    'slotindex 'slotindex 'slots slots
		    'custom (get index 'custom)
		    'pools (choice (pickstrings (get index 'pools))
				   (pool-source (pick (get index 'pools) pool?)))
		    'opts (get index 'opts)
		    'saved (gmtimestamp))))
	     (do-choices (slot slots)
	       (store! export slot (index-source (get index slot))))
	     export)))
	(else (irritant index '|NotSlotIndex|))))

(define (branch-table opts (size) (table))
  (default! size (getopt opts 'size 1000))
  (set! table (make-hashtable size))
  (if (getopt opts 'threadsafe #t)
      table
      (unsafe-hashtable table)))

(define (slotindex/branch slotindex (opts #f))
  (let* ((branchopts
	  (if (and opts (test slotindex 'opts))
	      (cons opts (get slotindex 'opts))
	      (or opts
		  (try (get slotindex 'opts) #f))))
	 (branch (frame-create #f 
		   'slotindex 'slotindex
		   'slots (get slotindex 'slots)
		   'root slotindex
		   'opts opts)))
    (do-choices (slot (get slotindex 'slots))
      (let ((index (slot->index slotindex slot)))
	(store! branch slot (branch-table (getopt branch 'opts)))))
    branch))

(defslambda (setup-branch-index branch-index slot)
  (try
   (get branch-index slot)
   (begin (slot->index (get branch-index 'root) slot)
     (store! branch-index slot (branch-table (getopt branch-index 'opts)))
     (get branch-index slot))))

(define (slotindex/merge! branch (parallel #f) (root) (count 0))
  (when (and (test branch 'slotindex 'slotindex)
	     (test branch 'root)
	     (test (get branch 'root) 'slotindex 'slotindex))
    (set! root (get branch 'root))
    (if parallel
	(choice-size
	 (threadjoin
	  (for-choices (slot (get branch 'slots))
	    (let ((table (get branch slot)))
	      (tryif (modified? table)
		(begin 
		  (store! branch slot 
			  (branch-table (getopt branch 'opts)
					(table-size table)))
		  (threadcall index-merge! (get root slot) table)))))))
	(begin (do-choices (slot (get branch 'slots))
		 (let ((table (get branch slot)))
		   (when (modified? table)
		     (store! branch slot 
			     (branch-table (getopt branch 'opts)
					   (table-size table)))
		     (index-merge! (get root slot) table)
		     (set! count (1+ count)))))
	  count))))
