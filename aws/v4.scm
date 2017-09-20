;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'aws/v4)

(use-module '{aws opts fdweb texttools logger varconfig curlcache})
(define %used_modules '{aws varconfig})

(define-init %loglevel %notice%)

(define-init v4err #f)
(varconfig! aws:v4err v4err)

(module-export! '{aws/v4/prepare aws/v4/get aws/v4/op v4err}) ;; aws/v4/post
(module-export! '{derive-key})

(define default-service "iam")

(define debug #t)

(define aws-regions
  {"us-east-1" "us-west-1" "us-west-2" "eu-west-1" "ap-southeast-1"
   "ap-southeast-2" "ap-northeast-1" "sa-east-1"})
(define aws-services
  {"sqs" "ses" "s3" "sns" "simpledb" "dynamodb" "ec2"})

(define-init aws-curlcache #f)
(varconfig! aws:curlcache aws-curlcache)

(define (getcurl)
  (if (not aws-curlcache) (frame-create #f)
      (if (symbol? aws-curlcache)
	  (curlcache/get aws-curlcache)
	  (curlcache/get 'awscurl))))

;;; Support functions

(define (derive-key secret date region service)
  (hmac-sha256
   (hmac-sha256 (hmac-sha256 (hmac-sha256 (glom "AWS4" secret) 
					  (get date 'isobasicdate))
			     region)
		service)
   "aws4_request"))

(define (encode-path string)
  (string-subst (uriencode string) "%2F" "/"))
(define (encode-uri string)
  (textsubst string
	     `#({"http://" "https://"}
		(not> "/")
		(subst (not> {"#" "?"}) ,encode-path)
		(rest))))

(define (get-string-to-sign date region service creq)
  (stringout "AWS4-HMAC-SHA256\n"
    (get date 'isobasic) "\n"
    (get date 'isobasicdate) "/" region "/" service "/aws4_request\n"
    (downcase (packet->base16 (sha256 creq)))))

(define (add-header! curl header . args)
  ((if (curl-handle? curl) curlsetopt! add!) 
   curl 'header (apply glom header ": " args)))

(define (urlop req op url curl (payload #f) (payload-mimetype #f))
  (onerror
      (cond ((equal? op "GET") (urlget url curl))
	    ((equal? op "HEAD") (urlhead url curl))
	    ((equal? op "POST") (urlpost url curl (or payload "")))
	    ((equal? op "PUT") 
	     (urlput url (or payload "") 
		     payload-mimetype
		     curl))
	    (else (urlget url curl)))
      (lambda (ex)
	(error AWS/V4/FAILED URLOP url req))))

;;; Doing a GET with AWS4 authentication

(define (aws/v4/get req endpoint (opts #f)
		    (args #[]) (headers #[]) (payload #f)
		    (curl) (date (gmtimestamp))
		    (token) (hdrset (make-hashset)))
  (default! curl (getopt opts 'handle (getcurl)))
  (aws/checkok req)
  (unless date (set! date (gmtimestamp)))
  (default! token
    (getopt req 'aws:token (getopt opts 'aws:token aws:token)))
  (add! req '%date date)
  (unless (test headers 'date)
    (add! headers 'date (get date 'isobasic)))
  (unless (test headers 'host)
    (add! headers 'host (urihost endpoint)))
  (when token
    (add! headers "X-Amz-Security-Token" token))
  (add! args "AWSAccessKeyId"
	(getopt req 'aws:key (getopt opts 'aws:key aws:key)))
  (add! args "Timestamp" (get date 'isobasic))
  (unless (position #\% endpoint)
    (set! endpoint (encode-uri endpoint)))
  (debug%watch "AWS/V4/GET" endpoint date args headers req opts)
  (process-args args req)
  (do-choices (key (getkeys headers))
    (unless (hashset-get hdrset (downcase key))
      (hashset-add! hdrset (downcase key))
      (let ((v (get headers key)))
	(if (curl-handle? curl)
	    (curlsetopt! curl 'header (glom key ": " v))
	    (add! curl 'header (cons key (qc v))))
	(add! req key
	      (if (and (singleton? v) (string? v)) v
		  (stringout (do-choices v
			       (if (timestamp? v) (get v 'isobasic)
				   (printout v))))))))
    (add! req '%headers key))
  ;; (add! args "SignatureMethod" "AWS-HMAC-SHA256")
  (set! req (aws/v4/prepare req "GET" endpoint opts (or payload "")))
  (when payload
    ((if (curl-handle? curl) curlsetopt! add!)
     curl 'header
     (glom "x-amz-content-sha256: "
       (if payload (downcase (packet->base16 (sha256 payload)))
	   "UNSIGNED-PAYLOAD"))))
  ((if (curl-handle? curl) curlsetopt! add!)
   curl 'header
   (glom "Authorization: AWS4-HMAC-SHA256 Credential=" (getopt req 'credential) ", "
     "SignedHeaders=" (signed-headers req) ", "
     "Signature=" (downcase (packet->base16 (getopt req 'signature)))))
  (info%watch "AWS/V4/get" endpoint args)
  (let* ((err (not (getopt req 'noerr (not (getopt opts 'err #t)))))
	 (url (scripturl+ endpoint args))
	 (result (urlget url curl))
	 (status (and result (try (get result 'response) #f))))
    (info%watch "AWS/V4/GOT" 
      err status "HEADER" (get result 'header))
    (if (and status (number? status) (>= 299 status 200))
	(cons result req)
	(if err 
	    (irritant (or (aws/error result req) (cons result req))
		|AWS/V4/Error| aws/v4/get)
	    (cons result req)))))

(define (aws/v4/op req op endpoint (opts #f)
		   (args #[]) (headers #[])
		   (payload #f) (payload-mimetype #f)
		   (date (gmtimestamp 'seconds))
		   (curl)
		   (token) (hdrset (make-hashset)))
  (default! curl (getopt opts 'handle (getcurl)))
  (aws/checkok req)
  (unless date (set! date (gmtimestamp 'seconds)))
  (default! token
    (getopt req 'aws:token (getopt opts 'aws:token aws:token)))
  (add! req '%date date)
  (unless (test headers 'date)
    (add! headers 'date (get date 'isobasic)))
  (unless (test headers 'host)
    (add! headers 'host (urihost endpoint)))
  (when token
    (add! headers "X-Amz-Security-Token" token))
  (add! args "AWSAccessKeyId"
	(getopt req 'aws:key (getopt opts 'aws:key aws:key)))
  (add! args "Timestamp" (get date 'isobasic))
  (debug%watch "AWS/V4/GET" endpoint date args headers req opts)
  (process-args args req)
  (when (getopt opts 'accept)
    (let ((curslot (pick (getkeys headers) downcase "accept")))
      (if (exists? curslot)
	  (store! headers curslot
		  (stringout (get headers curslot)  
		    (do-choices (type (getopt opts 'accept))
		      (printout "," type))))
	  (store! headers 'accept
		  (stringout 
		    (do-choices (type (getopt opts 'accept) i)
		      (printout (if (> i 0) ",") type)))))))
  (do-choices (key (getkeys headers))
    (unless (hashset-get hdrset (downcase key))
      (hashset-add! hdrset (downcase key))
      (let ((v (get headers key))
	    (capkey (capitalize (downcase key))))
	(if (curl-handle? curl)
	    (curlsetopt! curl 'header (glom capkey ": " v))
	    (add! curl 'header (cons capkey (qc v))))
	(add! req key
	      (if (and (singleton? v) (string? v)) v
		  (stringout (do-choices v
			       (if (timestamp? v) (get v 'isobasic)
				   (printout v))))))))
    (add! req '%headers key))
  ;; (add! args "Signature" (packet->base64 (getopt req 'signature)))
  ((if (curl-handle? curl) curlsetopt! add!) curl 'method (string->symbol op))
  ;; (add! args "SignatureMethod" "AWS-HMAC-SHA256")

  ;; TODO: For POST methods, the payload here should probably
  ;; www/url-form-encoded args
  (set! req  (aws/v4/prepare req op endpoint opts
			     (if (equal? op "GET") 
				 (or payload "")
				 (or payload args))
			     payload-mimetype))
  (when payload
    (add-header! curl "x-amz-content-sha256"
		 (downcase (packet->base16 (sha256 payload)))))
  (add-header! curl "Authorization" 
	       "AWS4-HMAC-SHA256 Credential="
	       (getopt req 'credential) ", "
	       "SignedHeaders=" (signed-headers req) ", "
	       "Signature=" (downcase (packet->base16 (getopt req 'signature))))
  (let* ((escaped (if (position #\% endpoint) endpoint
		      (encode-uri endpoint)))
	 (url (if (or (overlaps? op '{GET "GET" "get"}) payload)
		  (scripturl+ escaped args)
		  escaped)))
    (loginfo |AWS/V4/op| (write op) " " endpoint
	     "\n  params: " args "\n  headers: " headers "\n  "
	     (if (and payload (> (length payload) 0))
		 (printout (length payload)
		   (if (packet? payload) " bytes" " characters")
		   " of " (or payload-mimetype "stuff"))
		 "no payload")
	     "\n  url: " url "\n  curl: " curl)
    (let* ((result (urlop req op url curl payload payload-mimetype))
	   (err (not (getopt req 'noerr (not (getopt opts 'err #t)))))
	   (status (get result 'response)))
      (info%watch "AWS/V4/RESULT"
	err status "HEADER" (get result 'header))
      (cond ((not err) (cons result req))
	    ((and status (>= status 200) (< status 300))
	     ;; If the status is a version of OK, just cons the result
	     ;; onto the request.
	     (cons result req))
	    ((and status (equal? op "HEAD") (>= status 300) (< status 500))
	     ;; For HEAD requests, don't consider references as
	     ;; errors. Note that URLGET (in particular, CURL)
	     ;; automatically handles some number of redirects for non
	     ;; HEAD requests.
	     (cons result req))
	    (else (irritant (or (aws/error result req) (cons result req))
		      |AWS/V4/Error| aws/v4/op
		      ;; "\nop=" op "\nendpoint=" endpoint ", "
		      ;; "\nurl=" url "\ncurl=" curl
		      ))))))

(define (process-args args req)
  (do-choices (key (getkeys args))
    (if (and (string? key) (has-suffix key "*"))
	(let ((values (get args key))
	      (base (glom (slice key 0 -1) ".")))
	  (cond ((ambiguous? values)
		 (do-choices (val values i)
		   (add! args (glom base (1+ i)) val)
		   (add! req (glom base (1+ i)) val)
		   (add! req '%params (glom base (1+ i)))))
		((or (vector? values) (list? values))
		 (doseq (val values i)
		   (add! args (glom base (1+ i)) val)
		   (add! req (glom base (1+ i)) val)
		   (add! req '%params (glom base (1+ i)))))
		(else (add! args (glom base 1) values)
		      (add! req (glom base 1) values)
		      (add! req '%params (glom base 1))))
	  (drop! args key))
	(begin
	  (add! req key (get args key))
	  (add! req '%params key))))
  req)

;;; GENERATING KEYS, ETC

(define (aws/v4/prepare req method uri (opts #f) (payload #f) (ptype #f))
  (logdebug AWS/V4/PREPARE method " " uri "\n  " (pprint req))
  (let* ((host (getopt req 'host (urihost uri)))
	 (date (gmtimestamp
		(getopt req '%date
			(getopt req 'date
				(gmtimestamp 'seconds)))))
	 (region (try (getopt req '%region {})
		      (getopt req 'region {})
		      (getopt opts 'region (pick aws-regions search host))
		      aws:region))
	 (service (try (getopt req '%service {})
		       (getopt req 'service {})
		       (getopt opts 'service {})
		       (get (text->frames service-pat host) 'service)
		       default-service))
	 (awskey (getopt req 'aws:key (getopt opts 'aws:key aws:key)))
	 (awstoken (getopt req 'aws:token (getopt opts 'aws:token aws:token)))
	 (credential (glom awskey "/"
		       (get date 'isobasicdate) "/"
		       region "/" service "/aws4_request")))
    (unless (test req 'host) (store! req 'host host))
    (when (test req '%params "X-Amz-Algorithm")
      (store! req "X-Amz-Algorithm" "AWS4-HMAC-SHA256"))
    (when (test req '%params "X-Amz-Date")
      (store! req "X-Amz-Date" (get date 'isobasic)))
    (when (test req '%params "X-Amz-Credential")
      (store! req "X-Amz-Credential" credential))
    (when (test req '%params "X-Amz-SignedHeaders")
      (store! req "X-Amz-SignedHeaders" (signed-headers req)))
    (when (and (test req '%params "X-Amz-Security-Token") awstoken)
      (store! req "X-Amz-Security-Token" awstoken))
    (let* ((cq (if (or payload (eq? method "GET"))
		   (canonical-query-string req)
		   ""))
	   (ch (canonical-headers req))
	   (sh (signed-headers req))
	   (creq (stringout method "\n"
		   "/" (encode-path (uripath uri)) "\n"
		   cq "\n" ch "\n" sh "\n"
		   (if payload
		       (downcase (packet->base16 (sha256 payload)))
		       "UNSIGNED-PAYLOAD")))
	   (string-to-sign (get-string-to-sign date region service creq))
	   (secret (getopt req 'aws:secret aws:secret))
	   (signing-key (derive-key secret date region service))
	   (signature (hmac-sha256 signing-key string-to-sign)))
      (loginfo AWS/V4/PREPARE
	(write method) " " uri 
	(if (and payload (> (length payload) 0))
	    (printout "\n  " (length payload)
	      " " (if (packet? payload) "bytes " "characters ")
	      " of " (or ptype "stuff"))
	    "\n  no payload")
	"\n  creds=" (write credential)
	"\n  req=" (write creq)
	"\n  sts=" (write string-to-sign))
      (info%watch cq ch sh creq)
      (store! req 'aws:key awskey)
      (when awstoken (store! req 'aws:token awstoken))
      (store! req 'signature signature)
      (store! req 'region region)
      (store! req 'service service)
      (store! req 'date date)
      (store! req 'credential credential)
      (store! req 'host host)
      (when debug 
	(store! req 'creq creq)
	(store! req 'sigkey signing-key)
	(store! req 'string-to-sign string-to-sign))
      req)))

(define service-pat
  `(PREF
    #({(bos) "."}
      (label service (and (not> ".amazonaws.com") ,aws-services))
      ".amazonaws.com" (eos))
    #({(bos) "."}
      (label service ,aws-services)
      ".")))

;;; Canonical query

(define (canonical-query-string args (params))
  (default! params
    (getopt args '%params (getopt args 'params (getkeys args))))
  (let* ((pairs (for-choices (key params)
		  (cons (uriencode key)
			(for-choices (v (get args key))
			  (if (string? v) (uriencode v)
			      (if (timestamp? v) (get v 'isobasic)
				  (uriencode (stringout v)))))))))
    (stringout
      (doseq (q (sorted pairs car) i)
	(printout (if (> i 0) "&")
	  (car q) "=" (cdr q))))))

;;;; Canonical Headers

(define (merge-spaces s)
  (textsubst s '(isspace+) " "))

(define trim-rule
  `#((subst (not> "\"") ,merge-spaces)
     (* #("\"" (not> "\"") "\""
	  (subst (not> "\"") ,merge-spaces)))))

(define (trim-header-value value)
  (if (string? value)
      (trim-spaces (textsubst value trim-rule))
      value))
  
(define (canonical-headers args (headers))
  (default! headers 
    (getopt args '%headers (getopt args 'headers (getkeys args))))
  (let* ((pairs (for-choices (key headers)
		  (let ((v (get args key)))
		    (cons (trim-spaces (downcase key))
			  (if (string? v)
			      (trim-header-value v)
			      (trim-header-value
			       (if (timestamp? v) (get v 'isobasic)
				   (stringout v))))))))
	 (hkeys (sorted (car pairs))))
    (stringout
      (doseq (h hkeys i)
	(let ((v (get pairs h)))
	  (if (ambiguous? v)
	      (printout h ":"
		(doseq (ev (sorted v) i)
		  (printout (if (> i 0) ",") ev)))
	      (printout h ":" v "\n")))))))

(define (signed-headers args (headers))
  (default! headers 
    (getopt args '%headers (getopt args 'headers (getkeys args))))
  (let* ((keynames (trim-spaces (downcase headers)))
	 (hkeys (sorted keynames)))
    (stringout
      (doseq (h hkeys i)
	(printout (if (> i 0) ";") h)))))
