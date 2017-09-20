;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'reddit)

(use-module '{fdweb texttools reflection varconfig logger})
(use-module '{oauth})

(module-export! '{reddit/creds reddit.creds reddit.opts
		  reddit/get reddit/post reddit/more 
		  reddit/get/n reddit/comments
		  reddit/thing})

(define %loglevel %notice%)

(define reddit.oauth #f)
(define reddit.creds #f)

(define-init reddit.opts #[])

(define reddit:keep {})
(varconfig! reddit:keep reddit:keep)
(define reddit:drop {})
(varconfig! reddit:drop reddit:drop)

(define (reddit/creds)
  (or (and reddit.creds
	   (future? (getopt reddit.creds 'expires))
	   reddit.creds)
      (let* ((spec (if reddit.oauth
		       (if (pair? reddit.oauth)
			   reddit.oauth
			   (cons reddit.oauth (oauth/provider 'reddit)))
		       (oauth/provider 'reddit)))
	     (creds (oauth/getclient spec)))
	(set! reddit.creds creds)
	creds)))

(module-export! '{subreddits/search subreddits/new subreddits/popular
		  subreddits/default subreddits/gold
		  subreddits/subscribed})

(define (reddit/get endpoint (opts #f) (args #[]) (conn))
  (default! conn (getopt opts 'creds (reddit/creds)))
  (if opts
      (set! opts (cons opts reddit.opts))
      (set! opts reddit.opts))
  (when (getopt opts 'limit)
    (store! args "limit" (getopt opts 'limit)))
  (let* ((r (oauth/call conn 'GET endpoint args opts))
	 (response (car r))
	 (result (if (getopt opts 'raw #f)
		     response
		     (reddit/thing response opts))))
    (unless (or (getopt opts 'raw #f) (not (table? result))
		(not (test result 'listing)))
      (store! result 'endpoint endpoint)
      (store! result 'creds conn)
      (store! result 'opts opts)
      (store! result 'args args)
      (when (exists? (get result 'listing))
	(store! result 'count (choice-size (get result 'listing))))
      (lognotice |REDDIT/GET| 
	"Got " (choice-size (get result 'listing)) " results"))
    result))

(define (reddit/more response (endpoint) (opts) (args))
  (default! endpoint (get response 'endpoint))
  (default! opts (get response 'opts))
  (default! args (get response 'args))
  (store! args "after" (get response 'after))
  (when (test response 'count)
    (store! args "count" (get response 'count)))
  (when (getopt opts 'limit)
    (store! args "limit" (getopt opts 'limit)))
  (let* ((conn (getopt response 'creds
		       (getopt opts 'creds (reddit/creds))))
	 (r (oauth/call conn 'GET endpoint args opts))
	 (result (reddit/thing (car r) opts)))
    (unless (getopt opts 'raw #f)
      (store! result 'endpoint endpoint)
      (store! result 'creds conn)
      (store! result 'opts opts)
      (store! result 'args args)
      (lognotice |REDDIT/MORE| 
	"Got " (choice-size (get result 'listing)) 
	" results from " endpoint " beyond " (get response 'count))
      (when (exists? (get result 'listing))
	(store! result 'count (+ (try (get response 'count) 0)
				 (choice-size (get result 'listing))))))
    result))

(define (reddit/get/n endpoint (n 100) (opts #f) (args #[]) (conn) (results {}))
  (default! conn (getopt opts 'creds (reddit/creds)))
  (if opts
      (set! opts (cons opts reddit.opts))
      (set! opts reddit.opts))
  (let* ((fetch (reddit/get endpoint
			     (cons `#[limit ,(min n 100)] opts))))
    (while (and (exists? (get fetch 'listing))
		(< (get fetch 'count) n))
      (set+! results (get fetch 'listing))
      (set! fetch (reddit/more fetch)))
    (if (exists? (get fetch 'listing))
	(set+! results (get fetch 'listing))
	(logwarn |RedditFailed| 
	  "Reddit returned no more results"))
    results))

(define (reddit/post conn endpoint content (args #f))
  (let ((r (oauth/call conn 'GET endpoint args))
	(response (car r)))
    (reddit/thing response)))

(define (reddit/comments post (opts #f) (args #[]) (endpoint) (conn))
  (if opts
      (set! opts (cons opts reddit.opts))
      (set! opts reddit.opts))
  (default! conn (getopt opts 'creds (reddit/creds)))
  (when (getopt opts 'limit)
    (store! args "limit" (getopt opts 'limit)))
  (set! endpoint
	(cond ((and (string? post) (string-starts-with? post #/t[1-7]_/))
	       (glom "/comments/" (slice post 3)))
	      ((string? post) (glom "/comments/" post))
	      (else (glom "/comments/" (get post 'id)))))
  (let* ((r (oauth/call conn 'GET endpoint args opts))
	 (response (car r))
	 (result (if (getopt opts 'raw #f)
		     response
		     (reddit/thing response opts))))
    (if (vector? result)
	(elts result)
	result)))

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

(define reddit-type-map
  #["t1_" COMMENT "t2_" ACCOUNT "t3_" LINK "t4_" MESSAGE
    "t5_" SUBREDDIT "t6_" AWARD "t7_" CAMPAIGN])

(define (reddit/thing json (opts #f))
  (cond ((vector? json) 
	 (forseq (elt json) (reddit/thing elt opts)))
	((not (table? json)) json)
	((test json 'data)
	 (if (has-prefix (get json 'kind) "t")
	     (reddit/thing (get json 'data) opts)
	     (if (test (get json 'data) 'children)
		 (let ((data (reddit/thing (get json 'data) opts)))
		   (store! data (intern (upcase (get json 'kind)))
			   (get data 'children))
		   (drop! data 'children)
		   data)
		 (frame-create #f 
		   (intern (upcase (get json 'kind)))
		   (reddit/thing (get json 'data) opts)))))
	((test json 'children)
	 (frame-create #f
	   'children 
	   (for-choices (child (elts (get json 'children)))
	     (reddit/thing child opts))
	   'before (get json 'before) 'after (get json 'after)))
	(else (let* ((name (get json 'name))
		     (created (->exact (get json 'created)))
		     (created_utc (->exact (get json 'created_utc)))
		     (output (frame-create #f
			       'reddid name '%id (get json 'name)
			       'type (get reddit-type-map (slice name 0 3))))
		     (keys (getkeys json))
		     (xform (getopt opts 'xform {}))
		     (drop (choice (getopt opts 'drop {})
				   (cdr (pick (getopt opts 'pref {}) keys))
				   (getkeys xform)
				   'replies)))
		(store! output 'created
			(mktime created_utc 'gmtoff (- created created_utc)))
		(do-choices (xform-slot (pick keys xform))
		  (let ((v (get json xform-slot))
			(handler (get xform xform-slot)))
		    (cond ((symbol? handler)
			   (store! output handler v))
			  ((applicable? handler)
			   (store! output xform-slot (handler v)))
			  else)))
		(do-choices (slot (difference keys drop))
		  (let ((value (get json slot)))
		    (cond ((and (not (overlaps? slot reddit:keep))
				(or (fail? value)
				    (zero? value)
				    (overlaps? slot reddit:drop)
				    (overlaps? value {#f "" #()})
				    (and (table? value) (fail? (getkeys value))))))
			  ((string? value)
			   (store! output slot (decode-entities (get json slot))))
			  ((overlaps? slot '{created created_utc}))
			  ((not (table? value))
			   (store! output slot (reddit/thing value opts)))
			  ((test value 'listing)
			   (get value 'listing))
			  (else 
			   (store! output slot (reddit/thing value opts))))))
		(if (and (test output 'listing) (singleton (getkeys output)))
		    (get output 'listing)
		    output)))))


