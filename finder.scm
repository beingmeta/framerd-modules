;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'finder)

(use-module '{fdweb texttools varconfig regex logger})

(module-export! '{finder})

(define (find-matches dir spec (opts #f))
  (filter-choices (path (getfiles dir))
    (let ((base (basename path)))
      (cond ((regex? spec) 
	     (regex/search spec (basename path)))
	    ((string? spec)
	     (cond ((has-prefix spec "*") (has-suffix base (slice spec 1)))
		   ((has-suffix spec "*") (has-prefix base (slice spec 0 -1)))
		   (else (search spec base))))
	    ((or (pair? spec) (vector? spec))
	     (textsearch spec base)
	     (or (pair? spec) (vector? spec)))
	    (else (filename-match file base spec opts))))))

(define (filename-match file base spec opts)
  #f)

(define (finder dir match (opts #f) (root #f))
  (cond ((not root))
	((eq? root #t)
	 (set! root (getopt opts 'root (abspath dir))))
	((string? root))
	(else (irritant root '|InvalidRootArg|)))
  (when (and (string? root) (not (has-suffix root "/")))
    (set! root (glom root "/")))
  (if root
      (strip-prefix (find-matches dir match opts) root)
      (find-matches dir match opts)))

  


