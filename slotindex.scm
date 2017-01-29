;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2017 beingmeta, inc. All rights reserved

(in-module 'slotindex)

(use-module '{logger varconfig})

(module-export! '{slotindex/make slotindex/setup 
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
		   'opts opts)))
      (do-choices (slot (pick (elts slots) symbol?))
	(slotindex/setup base slot))
      base)))

(defslambda (slotindex/setup base slot)
  (try (get base slot)
       (let* ((dir (try (get base 'directory) rootdir))
	      (prefix (try (get base 'prefix) prefix))
	      (indexopts (try (get base 'opts) (cons #[] default-opts)))
	      (custom (try (get (getopt indexopts 'custom {}) slot)
			   (get (getopt default-opts 'custom {}) slot)
			   #[]))
	      (opts (cons custom indexopts))
	      (path (getopt opts 'fullpath
			    (mkpath dir (getopt opts 'basename
						(glom prefix (downcase slot) ".index")))))
	      (index #f))
	 (when (and (config 'slotindex:restart #f)
		    (file-exists? path))
	   (remove-file path))
	 (unless (file-exists? path)
	   (lognotice |NewIndex|
	     "Creating new index for " slot " at " path)
	   (make-hash-index path (getopt opts 'size)
			    (getopt opts 'flags '()) slot 
			    (qc (getopt opts 'baseoids {})
				(pool-base (getopt opts 'pools {})))))
	 (set! index (open-index path))
	 (store! base slot index)
	 (add! base 'slots slot)
	 index)))

(define (slot->index meta-index slot)
  (try (get meta-index slot)
       (slotindex/setup meta-index slot)))

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
	 (logwarn |IndexSave| "Saving " index))
	((test index 'slotindex 'slotindex)
	 (let* ((slots (get index 'slots))
		(indices (get index (get index 'slots)))
		(tosave (pick indices modified?)))
	   (logwarn |IndexSave| 
	     "Saving indices for " 
	     (choice-size tosave) "/" (choice-size indices)
	     " slots: " (do-choices (slot (pick slots index tosave) i)
			  (printout (if (> i 0) ", ") slot)))
	   (thread/join (thread/call commit indices))))
	(else)))


