;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'reddit)

(use-module '{fdweb texttools reflection varconfig logger})
(use-module '{oauth})

(module-export! '{reddit-key reddit-secret reddit/get reddit/post reddit/thing})

(define reddit-key #f)
(varconfig! reddit:key reddit-key)
(define reddit-secret #f)
(varconfig! reddit:secret reddit-secret)

(module-export! '{subreddits/search subreddits/new subreddits/popular
		  subreddits/default subreddits/gold
		  subreddits/subscribed})

(define (reddit/get conn endpoint (args #f))
  (let* ((r (oauth/call conn 'GET endpoint args))
	 (response (car r)))
    (reddit/thing response)))

(define (reddit/post conn endpoint content (args #f))
  (let ((r (oauth/call conn 'GET endpoint args))
	(response (car r)))
    (reddit/thing response)))

(define (subreddits/search cl string)
  (oauth/call cl 'GET "/subreddits/search" `#["q" ,string]))
(define (subreddits/new cl)
  (oauth/call cl 'GET "/subreddits/new"))
(define (subreddits/popular cl)
  (oauth/call cl 'GET "/subreddits/popular"))
(define (subreddits/gold cl)
  (oauth/call cl 'GET "/subreddits/gold"))
(define (subreddits/default cl)
  (oauth/call cl 'GET "/subreddits/default"))

(define (reddit/thing json (pool #f))
  (cond ((vector? json) (forseq (elt json) (reddit/thing elt pool)))
	((not (table? json)) json)
	((test json 'data)
	 (if (has-prefix (get json 'kind) "t")
	     (reddit/thing (get json 'data))
	     (if (test (get json 'data) 'children)
		 (let ((data (reddit/thing (get json 'data))))
		   (store! data (intern (upcase (get json 'kind)))
			   (get data 'children))
		   (drop! data 'children)
		   data)
		 (frame-create #f 
		   (intern (upcase (get json 'kind)))
		   (reddit/thing (get json 'data))))))
	((test json 'children)
	 (frame-create #f
	   'children 
	   (for-choices (child (elts (get json 'children)))
	     (reddit/thing child pool))
	   'before (get json 'before) 'after (get json 'after)))
	(else (let ((output (frame-create pool 'name (get json 'name))))
		(do-choices (slot (getkeys json))
		  (let ((value (get json slot)))
		    (cond ((or (fail? value)
			       (overlaps? value {#f "" #()})
			       (and (table? value) (fail? (getkeys value)))))
			  ((string? value)
			   (store! output slot (decode-entities (get json slot))))
			  ((overlaps? slot '{created created_utc})
			   (unless (timestamp? value)
			     (store! output slot (timestamp (->exact value)))))
			  (else (store! output slot (reddit/thing value))))))
		output))))

