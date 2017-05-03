;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'registry)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger})
(define %used_modules 'ezrecords)

(module-export! '{use-registry set-registry! register registry/ref})

;; Not yet used
(define default-registry #f)

(define (registry->string r)
  (stringout "#<REGISTRY " (registry-slotid r)
    (if (registry-server r) 
	(write (registry-server r))
	(printout (pool-id (registry-pool r)) "/"
	  (index-id (registry-index r))))
    ">"))

(define-init registries (make-hashtable))

(defrecord (registry OPAQUE `(stringfn . registry->string))
  slotid server pool index 
  (cache (make-hashtable))
  (lock (make-condvar))
  (opts #[]))

(defslambda (register-registry slotid server pool index (replace #f))
  (when replace (drop! registries slotid))
  (try (get registries slotid)
       (let ((registry (cons-registry server pool index)))
	 (store! registries slotid registry)
	 registry)))

(define (use-registry slotid spec (second-spec #f) (third-spec #f))
  (try (get registries slotid)
       (let* ((server (and (exists (position {#\@ #\:} spec))
			   (open-dtserver spec)))
	      (pool (if (and server second-spec) 
			(use-pool second-spec)
			(use-pool spec)))
	      (index (open-index (or third-spec second-spec spec))))
	 (register-registry slotid server pool index))))
(define (set-registry! slotid spec (second-spec #f))
  (if (and (test registries slotid) (string? spec)
	   (exists (position {#\@ #\:} spec))
	   (registry-server (get registries slotid))
	   #f)
      (get registries slotid)
      (let* ((server (and (exists (position {#\@ #\:} spec))
			  (open-dtserver spec)))
	     (pool (if (and server second-spec) 
		       (use-pool second-spec)
		       (use-pool spec)))
	     (index (if second-spec 
			(open-index second-spec)
			(open-index spec))))
	(if (test registries slotid)
	    (let ((cur (get registries slotid)))
	      (if (and (equal? server (registry-server cur))
		       (equal? pool (registry-pool cur))
		       (equal? index (registry-index cur)))
		  cur
		  (register-registry slotid server pool index #t)))
	    (register-registry slotid server pool index #t)))))

(define (registry/get registry slotid value (create #f))
  (with-lock (registry-lock registry)
    (try (get (registry-cache registry) value)
	 (let ((existing (find-frames (registry-index registry)
			   slotid value))
	       (result (try existing
			    (tryif create
			      (if (registry-server registry)
				  (dtcall (registry-server registry)
					  'register slotid value)
				  (frame-create (registry-pool registry)
				    '%id (list slotid value)
				    slotid value))))))
	   (when (exists? result)
	     (when (and (fail? existing) (not (registry-server registry)))
	       (index-frame (registry-index registry) result slotid value))
	     (store! (registry-cache registry) value result))
	   result))))  

(define (registry/ref registry slotid value (create #f))
  (default! registry (try (get registries slotid) #f))
  (if registry
      (try (get (registry-cache registry) value)
	   (registry/get registry slotid value #f))
      (irritant slotid "No Registry"
	"No registry exists for the slot " (write slotid))))

(define (register slotid value (registry))
  (default! registry (try (get registries slotid) #f))
  (if registry
      (try (get (registry-cache registry) value)
	   (registry/get registry slotid value #t))
      (irritant slotid "No Registry"
	"No registry exists for the slot " (write slotid))))


