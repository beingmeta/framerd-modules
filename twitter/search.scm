;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'twitter/search)

(use-module '{oauth varconfig logger})
(use-module '{twitter})

(module-export! '{twitter/searchapi
		  twitter/search
		  twitter/search/n})

(define %loglevel %warn%)

;;(load-config (get-component "listenbot.cfg"))

(define search-endpoint (glom twitter-root "search/tweets.json"))

(define (twitter/searchapi args (access (twitter/creds)))
  (oauth/call access 'GET search-endpoint args))

(define (getid x) (get x 'id))

(define (twitter/search q (opts #f) (fetchsize) (creds))
  (when (fixnum? opts)
    (set! fetchsize opts)
    (set! opts #[]))
  (default! fetchsize
    (if (table? q)
	(getopt q "count" (getopt opts 'fetchsize 100))
	(getopt opts 'fetchsize 100)))
  (default! creds (getopt opts 'creds (twitter/creds)))
  (when (string? q)
    (set! q (frame-create #f "q" q "count" fetchsize)))
  (let* ((start (elapsed-time))
	 (creds (if (and creds (pair? creds)) creds
		    (try (get q 'creds) (twitter/creds))))
	 (r (oauth/call creds 'GET search-endpoint (try (get q 'next) q)))
	 (tweets (get r 'statuses))
	 (ids (get (elts tweets) 'id))
	 (min_id (smallest ids))
	 (max_id (largest ids)))
    (loginfo |Twitter/search| 
      "Calling twitter with " (try (get q 'next) q))
    (lognotice |Twitter/search| 
      "Got " (length tweets) " tweets for " (write (getopt q "q")) 
      " in " (secs->string (elapsed-time start)) " based on:\n"
      (pprint q))
    `#[q ,q count ,fetchsize
       creds ,creds min_id ,min_id max_id ,max_id
       tweets ,(get r 'statuses)
       %original ,(get r '%original)
       next #["q" ,(get q "q") "count" ,fetchsize
	      "max_id" ,(-1+ min_id)]]))

(define (twitter/search/n q (opts #f) (n) (blocksize) (creds (twitter/creds)))
  (when (fixnum? opts) (set! n opts) (set! opts #f))
  (default! n 
    (try (getopt opts 'count)
	 (tryif (table? q) (get q "count"))
	 100))
  (default! blocksize (getopt opts 'blocksize 100))
  (when (> blocksize n) (set! blocksize n))
  (info%watch "TWITTER/SEARCH/N" q opts n blocksize)
  (if (string? q)
      (set! q `#["q" ,q "count" ,blocksize])
      (store! q "count" blocksize))
  (let* ((start (elapsed-time))
	 (search_min (getopt opts 'min_id (try (get q "since_id") #f)))
	 (search_max (getopt opts 'max_id (try (get q "max_id") #f)))
	 (qstring (get q "q"))
	 (backward (or (< n 0) (< blocksize 0) search_max))
	 (result #f)
	 (blocks '())
	 (done #f)
	 (count 0))
    (if backward
	(when search_max (store! q "max_id" search_max))
	(when search_min (store! q "since_id" search_min)))
    (loginfo |Twitter/search/n| 
      "Calling API at count=" count "/" n " with "
      (if (table? q) q `#["q" ,qstring "count" ,blocksize]))
    (set! result 
	  (oauth/call creds 'GET search-endpoint
		      (if (table? q) q `#["q" ,qstring "count" ,blocksize])))
    (while (and (not done) (< count n) (exists? result) (table? result))
      (let* ((tweets (and (exists? result) result (table? result)
			  (test result 'statuses)
			  (get result 'statuses)))
	     (metadata (get result 'search_metadata))
	     (ids (elts (map (lambda (x) (get x 'id)) tweets)))
	     (min_id (smallest ids))
	     (max_id (largest ids)))
	(set! search_max (if (not search_max) max_id (max search_max max_id)))
	(set! search_min (if (not search_min) min_id (min search_min min_id)))
	(if (= (length tweets) 0)
	    (begin (loginfo |Twitter/search/n| "Search returned zero results")
	      (set! done #t))
	    (set! blocks (cons tweets blocks)))
	(set! count (+ count (length tweets)))
	(unless done
	  (loginfo |Twitter/search/n| 
	    "Calling API at count=" count "/" n " with "
	    `#["q" ,(get q "q") "count" ,blocksize
	       ,(if backward "max_id" "since_id")
	       ,(if backward (-1+ min_id) max_id)])
	  (set! result
		(oauth/call creds 'GET search-endpoint
			    `#["q" ,(get q "q") "count" ,blocksize
			       ,(if backward "max_id" "since_id")
			       ,(if backward (-1+ min_id) max_id)])))))
    (lognotice |Twitter/Search/N| 
      "Got " count "/" n " tweets for " (write qstring) 
      " in " (secs->string (elapsed-time start)))
    (apply append blocks)))
