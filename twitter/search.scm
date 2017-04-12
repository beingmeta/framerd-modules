;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'twitter/search)

(use-module '{oauth varconfig logger})

(module-export! '{twitter/searchapi
		  twitter/search
		  twitter/search/n})

;;(load-config (get-component "listenbot.cfg"))

(define search-endpoint "https://api.twitter.com/1.1/search/tweets.json")

(define (twitter/searchapi args (access (twitter/creds)))
  (oauth/call access 'GET search-endpoint args))

(define (getid x) (get x 'id))

(define (twitter/search q (blocksize) (creds #f))
  (default! blocksize 
    (if (table? q) 
	(getopt q 'count (getopt q 'blocksize 5000))
	5000))
  (when (string? q)
    (set! q (frame-create "q" q "count" blocksize)))
  (let* ((creds (if (and creds (pair? creds)) creds
		    (try (get q 'creds) (twitter/creds))))
	 (r (oauth/call creds 'GET search-endpoint (try (get q 'next) q)))
	 (tweets (get r 'statuses))
	 (min_id (smallest (get (elts tweets) 'id))))
    `#[tweets ,(get r 'statuses) q ,q count ,blocksize
       creds ,creds
       next #["q" ,(get q "q") "count" ,blocksize
	      "max_id" ,(-1+ min_id)]]))

(define (twitter/search/n q n (blocksize) (creds (twitter/creds)))
  (default! blocksize (quotient n 10))
  (let ((blocks '())
	(done #f)
	(count 0)
	(result (oauth/call creds 'GET search-endpoint
			    `#["q" ,q "count" ,blocksize])))
    (while (and (not done) (< count n) (exists? result) (table? result))
      (let* ((tweets (and (exists? result) result (table? result)
			  (test result 'statuses)
			  (get result 'statuses)))
	     (metadata (get result 'search_metadata))
	     (min_id (smallest (elts (map (lambda (x) (get x 'id)) tweets)))))
	(logwarn |GotTweets| "Got " (length tweets) " tweets, total=" (+ count (length blocks)))
	(set! blocks (cons tweets blocks))
	(set! count (+ count (length tweets)))
	(set! result (oauth/call creds 'GET search-endpoint
				 `#["q" ,q "count" ,blocksize
				    "max_id" ,(-1+ min_id)]))))
    (apply append blocks)))
