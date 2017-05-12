;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'registry)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger varconfig})
(define %used_modules 'ezrecords)

(module-export! '{use-registry set-registry!
		  register registry/ref
		  registry/save!})

(module-export! '{registry-slotid registry-spec
		  registry-pool registry-index registry-server})

(define %loglevel %notice%)

;; Not yet used
(define default-registry #f)

;; This determines whether the pool should use a bloom filter to
;; optimize registration
(define use-bloom #t)
(varconfig! registry:bloom use-bloom)

(define (registry->string r)
  (stringout "#<REGISTRY " (registry-slotid r) " " (registry-spec r) ">"))

(define-init registries (make-hashtable))

(defrecord (registry OPAQUE `(stringfn . registry->string))
  slotid spec server pool index (slotid #f) (slotindexes {})
  (idfile #f) (bloom #f)
  (newids (make-hashset))
  (cache (make-hashtable))
  (lock (make-condvar)))
(module-export! '{registry? registry-pool registry-index registry-server})

(defambda (register slotid value 
		    (inits #f) (defaults #f) (adds #f)
		    (registry-arg #f)
		    (reg))
  (unless registry-arg
    (do-choices slotid
      (unless (get registries slotid)
	(irritant slotid |No Registry| REGISTER
		  "No registry exists for the slot "
		  (write slotid)))))
  (for-choices slotid
    (set! reg (or registry-arg (get registries slotid)))
    (for-choices value
      (cond ((and reg (not defaults) (not adds))
	     ;; Simple call, since we dont' need to do anything 
	     ;; with the object after we get it.
	     (try (get (registry-cache reg) value)
		  (registry/get reg slotid value inits)))
	    (reg
	     (let ((frame 
		    (try (get (registry-cache reg) value)
			 (registry/get reg slotid value inits)))
		   (slotindexes (registry-slotindexes reg)))
	       (when defaults
		 (when (test defaults '%id) 
		   (store! frame '%id (get defaults '%id)))
		 (do-choices (slotid (getkeys defaults))
		   (unless (test frame slotid)
		     (store! frame slotid (get defaults slotid))
		     (if (overlaps? slotid slotindexes)
			 (index-frame index frame slotid)
			 (if (test (pick slotindexes table?) slotid)
			     (index-frame (get slotindexes slotid)
				 frame slotid))))))
	       (when adds
		 (do-choices (slotid (getkeys adds))
		   (add! frame slotid (get adds slotid))
		   (if (overlaps? slotid slotindexes)
		       (index-frame index frame slotid (get adds slotid))
		       (if (test (pick slotindexes table?) slotid)
			   (index-frame (get slotindexes slotid)
			       frame slotid (get adds slotid))))))
	       frame))
	    (else (fail))))))

(define (registry/ref slotid value (registry))
  (default! registry (try (get registries slotid) #f))
  (unless registry (set! registry (try (get registries slotid) #f)))
  (if registry
      (try (get (registry-cache registry) value)
	   (registry/get registry slotid value #f))
      (irritant slotid |No Registry| registry/ref
		"No registry exists for the slot " (write slotid))))

(define (registry/save! (r #f))
  (when (and (or (symbol? r) (oid? r)) (test registries r))
    (set! r (get registries r)))
  (if (and r (registry-server r))
      (logwarn |RemoteRegistry| "Can't save a remote registry")
      (let* ((r (or r (pick (get registries (getkeys registries))
			    registry-server #f)))
	     (pools (registry-pool r))
	     (indexes (registry-index r))
	     (adjuncts-map (get-adjuncts pools))
	     (adjuncts (get adjuncts-map (getkeys adjuncts-map)))
	     (threads
	      (choice (thread/call+ #[logexit #f]
			  commit {pools indexes adjuncts})
		      (thread/call+ #[logexit #f] save-ids! r))))
	(if (exists? threads)
	    (threadjoin threads)
	    (logwarn |NoRegistry| "Couldn't get a registry to save")))))

(define (save-ids! r)
  (let ((elts (hashset-elts (registry-newids r) #t)))
    (dtype->file+ elts (registry-idfile r))))

;;; The meat of it

(define (registry/get registry slotid value (create #f) (server) (index))
  (default! server (registry-server registry))
  (default! index (registry-index registry))
  (with-lock (registry-lock registry)
    (try (get (registry-cache registry) value)
	 (if server
	     (try (find-frames index slotid value)
		  (dtcall server 'register slotid value))
	     (let* ((bloom (registry-bloom registry))
		    (key (if (registry-slotid registry) 
			     value
			     (cons slotid value)))
		    (existing (if (and bloom (not (bloom/check bloom key)))
				  (fail)
				  (find-frames index slotid value)))
		    (result (try existing
				 (tryif create
				   (frame-create (registry-pool registry)
				     '%id (list slotid value)
				     slotid value)))))
	       (when (exists? result)
		 (when (fail? existing)
		   (index-frame index result 'has slotid)
		   (index-frame index result slotid value)
		   (when (table? create)
		     (do-choices (key (getkeys create))
		       (store! result key (get create key)))))
		 (when bloom
		   (hashset-add! (registry-newids registry) key)
		   (bloom/add! bloom key))
		 (store! (registry-cache registry) value result))
	       result)))))

;;; Registering registries

(defslambda (register-registry-inner slotid spec (replace #f))
  (when replace (drop! registries slotid))
  (try (get registries slotid)
       (if (registry? spec)
	   (if (and (registry-slotid spec) 
		    (not (eq? (registry-slotid spec) slotid)))
	       (irritant spec
		   |SingleSlotRegistry| |register-registry| 
		   "The registry " registry " is configured for the slot "
		   (registry-slotid spec) " which isn't " slotid)
	       (begin (store! registries slotid spec)
		 spec))
	   (let ((server (and (getopt spec 'server)
			      (if (dtserver? (getopt spec 'server))
				  (getopt spec 'server)
				  (open-dtserver (getopt spec 'server)))))
		 (pool (try (use-pool (getsource spec 'pool)) #f))
		 (index (try (open-index (getsource spec 'index)) #f))
		 (registry #f))
	     (if (or server (not (getopt spec 'bloom use-bloom)))
		 ;; Server-based registries don't (currently) have
		 ;; bloom filters or idstreams
		 (set! registry
		       (cons-registry slotid spec server pool index
				      (getopt spec 'slotid)
				      (qc (getopt spec 'slotindex {}))))
		 (let* ((ixsource (and index (index-source index)))
			(idbase (and ixsource 
				     (not (∃ position {#\: #\@} ixsource))
				     ixsource))
			(idpath (getopt spec 'idpath 
					(get-idpath spec idbase)))
			(slotid (has-suffix idpath ".ids"))
			(idstream (and idpath 
				       (or (file-exists? idpath)
					   (getopt spec 'bloom use-bloom))
				       (extend-dtype-file idpath)))
			(bloom (and idstream (get-bloom idpath))))
		   (set! registry
			 (cons-registry slotid spec server pool index slotid 
					(qc (getopt spec 'slotindex {}))
					idstream bloom))))
	     (store! registries slotid registry)
	     registry))))

(define (getsource spec slot)
  (try
   (getopt spec slot {})
   (let ((server (getopt spec 'server)))
     (cond ((not server) {})
	   ((string? server) server)
	   ((dtserver? server) (dtserver-id server))
	   (else {})))
   (getopt spec 'source {})))

(define (register-registry slotid spec (replace #f))
  (if replace
      (let ((cur (get registries slotid))
	    (new (if (registry? spec) 
		     (if (and (registry-slotid spec)
			      (not (eq? slotid (registry-slotid spec))))
			 (irritant spec
			     |SingleSlotRegistry| |register-registry| 
			     "The registry " registry " is configured for the slot "
			     (registry-slotid spec) " which isn't " slotid)
			 spec)
		     (register-registry-inner slotid spec replace))))
	(if (fail? new)
	    (logcrit |RegisterRegistry|
	      "Couldn't create registry for " slotid 
	      " specified as " spec)
	    (unless (identical? cur new)
	      (lognotice |RegisterRegistry|
		"Registry for " slotid " is now "
		(if (registry-server new)
		    (dtserver-id (registry-server new))
		    (pool-id (registry-pool new)))))
	    new))
      (try (get registries slotid)
	   (let ((new (register-registry-inner slotid spec replace)))
	     (lognotice |RegisterRegistry|
	       "Registry for " slotid " is now "
	       (if (registry-server new)
		   (dtserver-id (registry-server new))
		   (pool-id (registry-pool new))))
	     new))))

(define (get-idpath spec idbase (root))
  (default! root (strip-suffix idbase ".index"))
  (cond ((file-exists? (glom idbase ".keys"))
	 (when (getopt spec 'slotid)
	   (logwarn |MultiSlotRegistry| 
	     "The registry is already using the multi-slot key file "
	     (glom idbase ".keys")))
	 (glom idbase ".keys"))
	((file-exists? (glom idbase ".ids"))
	 (glom idbase ".ids"))
	((file-exists? (glom root ".keys"))
	 (when (getopt spec 'slotid)
	   (logwarn |MultiSlotRegistry| 
	     "The registry is already using the multi-slot key file "
	     (glom idbase ".keys")))
	 (glom root ".keys"))
	((file-exists? (glom root ".ids"))
	 (glom root ".ids"))
	((getopt spec 'slotid) (glom idbase ".ids"))
	(else (glom idbase ".keys"))))

(define (registry-opts arg)
  (cond ((table? arg) (fixup-opts arg))
	((index? arg) (registry-opts (strip-suffix (index-id arg) ".pool")))
	((pool? arg)  (registry-opts (strip-suffix (pool-id arg) ".index")))
	((not (string? arg)) (irritant arg |InvalidRegistrySpec|))
	((exists position {#\: #\@} arg) `#[server ,arg])
	(else `#[server #f pool ,arg index ,arg slotid #f])))

(define (fixup-opts opts (source))
  (default! source (getopt opts 'source))
  (when (string? source)
    (when (and (not (getopt opts 'server))
	       (exists position {#\: #\@} source))
      (store! opts 'server source))
    (unless (getopt opts 'pool)
      (store! opts 'pool source))
    (unless (getopt opts 'index)
      (store! opts 'index source)))
  opts)

(define (use-registry slotid spec)
  (info%watch "USE-REGISTRY" slotid spec)
  (when (string? spec) (set! spec (registry-opts spec)))
  (try (get registries slotid)
       (register-registry slotid spec)))

(define (need-replace? registry spec)
  (or (and (getopt spec 'server) (not (registry-server registry)))
      (and (registry-server registry) (not (getopt spec 'server)))
      (if (registry-server registry)
	  (or (eq? (getopt spec 'server) (registry-server registry))
	      (and (dtserver? (getopt spec 'server))
		   (equal? (dtserver-id (registry-server registry))
			   (dtserver-id (getopt spec 'server))))
	      (equal? (dtserver-id (registry-server registry))
		      (getopt spec 'server)))
	  (or (and (not (registry-bloom registry))
		   (getopt spec 'bloom use-bloom))
	      (and (registry-bloom registry)
		   (not (getopt spec 'bloom use-bloom)))
	      (not (equal? (use-pool (get spec 'pool))
			   (registry-pool registry)))
	      (not (equal? (open-index (get spec 'index))
			   (registry-index registry)))))))

(define (set-registry! slotid spec)
  (info%watch "SET-REGISTRY!" slotid spec)
  (when (string? spec) (set! spec (registry-opts spec)))
  (if (test registries slotid)
      (when (need-replace? (get registries slotid) spec)
	(register-registry slotid spec #t))
      (register-registry slotid spec #t)))

;;; Getting the ids for a bloom filter

(define (get-bloom path (error #f))
  (if (and (string? path) (file-exists? path))
       (let* ((ids (file->dtypes path))
	      (bloom (make-bloom-filter (* 8 (max (choice-size ids) 4000000)) 
					(or error 0.0001))))
	 (bloom/add! bloom ids)
	 bloom)
       (make-bloom-filter 8000000 (or error 0.0001))))



