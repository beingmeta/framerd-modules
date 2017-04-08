;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'twitter/search)

(use-module '{oauth varconfig logger})

(module-export! '{twitter/searchapi
		  twitter/search
		  twitter/search/n})

;;(load-config (get-component "listenbot.cfg"))

(define default-creds 'twitter20)
(define search-endpoint "https://api.twitter.com/1.1/search/tweets.json")

(define (twitter/searchapi args (access default-token))
  (oauth/call access 'GET search-endpoint args))

(define (twitter/search q (blocksize) (token default-token))
  (default! blocksize 
    (if (table? q) (getopt q 'blocksize 5000) 5000))
  (when (string? q)
    (set! q (frame-create "q" q "count" blocksize)))
  (oauth/call token 'GET search-endpoint q))

(define (twitter/search/n q n (blocksize) (token default-token))
  (default! blocksize (quotient n 10))
  (let ((blocks '())
	(done #f)
	(count 0)
	(result (oauth/call token 'GET search-endpoint
			    `#["q" ,q "count" ,blocksize])))
    (while (and (not done) (< count n) (exists? result) (table? result))
      (let* ((tweets (and (exists? result) result (table? result)
			  (test result 'statuses)
			  (get result 'statuses)))
	     (metadata (get result 'search_metadata))
	     (max_id (get metadata 'max_id)))
	(logwarn |GotTweets| "Got " (length tweets) " tweets, total=" (+ count (length blocks)))
	(set! blocks (cons tweets blocks))
	(set! count (+ count (length tweets)))
	(set! result (oauth/call token 'GET search-endpoint
				 `#["q" ,q "count" ,blocksize
				    "max_id" ,(-1+ max_id)]))))
    (apply append blocks)))




