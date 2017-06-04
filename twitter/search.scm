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
	 (max_id (getopt opts 'max_id (try (get q "max_id") #f)))
	 (qstring (get q "q"))
	 (result #f)
	 (blocks '())
	 (done #f)
	 (count 0))
    (when max_id (store! q "max_id" max_id))
    (loginfo |Twitter/search/n| 
      "Calling API at count=" count "/" blocksize "/" n 
      " max_id=" max_id " and q=" q)
    (set! result 
	  (if max_id
	      (oauth/call creds 'GET search-endpoint
			  `#["q" ,qstring "count" ,blocksize "max_id" max_id])
	      (oauth/call creds 'GET search-endpoint
			  `#["q" ,qstring "count" ,blocksize])))
    (while (and (exists? result) result (table? result))
      (debug%watch "Twitter/Search/N" done count n 
		   "n_results" (length (get result 'statuses)))
      (let* ((tweets (and (exists? result) result (table? result)
			  (test result 'statuses)
			  (get result 'statuses)))
	     (metadata (get result 'search_metadata))
	     (ids (elts (map (lambda (x) (get x 'id)) tweets)))
	     (min_result (smallest ids))
	     (new_max_id (if max_id (min max_id min_result) min_result)))
	(set! max_id (try (-1+ new_max_id) #f))
	(if (= (length tweets) 0)
	    (begin (loginfo |Twitter/Search/N| 
		     "Search returned zero results, stopping at "
		     count "/" blocksize "/" n)
	      (set! done #t))
	    (set! blocks (cons tweets blocks)))
	(set! count (+ count (length tweets)))
	(set! result #f)
	(when (>= count n) (set! done #t))
	(unless done
	  (loginfo |Twitter/Search/N| 
	    "Calling API at count=" count "/" n " with "
	    `#["q" ,qstring "count" ,blocksize
	       "max_id" ,max_id])
	  (set! result 
		(if max_id
		    (oauth/call creds 'GET search-endpoint
				`#["q" ,qstring "count" ,blocksize "max_id" ,max_id])
		    (oauth/call creds 'GET search-endpoint
				`#["q" ,qstring "count" ,blocksize]))))))
    (lognotice |Twitter/Search/N| 
      "Got " count "/" n " tweets for " (write qstring) 
      " in " (secs->string (elapsed-time start)))
    (apply append blocks)))


