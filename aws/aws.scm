;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'aws)

(use-module '{logger opts texttools fdweb gpath varconfig})

(define-init %loglevel %notice%)

(define %nosubst '{aws:account
		   aws:key aws:secret aws:expires
		   aws/refresh aws:token})

(module-export! 
 '{aws:account aws:key aws:secret aws:token aws:expires
   aws/ok? aws/checkok aws/set-creds! aws/creds!
   aws/datesig aws/datesig/head aws/template
   aws/update-creds!
   aws/error
   aws:region})

(define aws:region "us-east-1")
(varconfig! aws:region aws:region)

;;; Templates source
(define template-sources
  (list (dirname (get-component "templates/template.json"))))
(varconfig! aws:templates template-sources #f cons)

;; Default (non-working) values from the environment
(define-init aws:secret
  (and (config 'dotload) (getenv "AWS_SECRET_ACCESS_KEY")))
(define-init aws:key
  (and (config 'dotload) (getenv "AWS_ACCESS_KEY_ID")))
(define-init aws:account
  (and (config 'dotload (getenv "AWS_ACCOUNT_NUMBER"))))

(config-def! 'aws:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws:secret val)
		   aws:secret)))
(config-def! 'aws:key
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws:key val)
		   aws:key)))
(config-def! 'aws:account
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws:account val)
		   aws:account)))

(define-init aws:token #f)
(define-init aws:expires #f)
(define-init aws/refresh #f)

(when (config 'aws:config)
  (if (file-exists? (config 'aws:config))
      (load-config (config 'aws:config))
      (logwarn |MissingAWSConfig|
	"The file " (config 'aws:config) "doesn't exist")))

(define (aws/datesig (date (timestamp)) (spec #{}))
  (unless date (set! date (timestamp)))
  (default! method (try (get spec 'method) "HmacSHA1"))
  ((if (test spec 'algorithm "HmacSHA256") hmac-sha256 hmac-sha1)
   (try (get spec 'secret) aws:secret)
   (get (timestamp date) 'rfc822)))

(define (aws/datesig/head (date (timestamp)) (spec #{}))
  (stringout "X-Amzn-Authorization: AWS3-HTTPS"
    " AWSAccessKeyId=" (try (get spec 'accesskey) aws:key)
    " Algorithm=" (try (get spec 'algorithm) "HmacSHA1")
    " Signature=" (packet->base64 (aws/datesig date spec))))

(define (aws/ok? (opts #f) (err #f))
  (if (or (not opts) (not (getopt opts 'aws:secret)))
      (if (not aws:secret)
	  (and err (error |NoAWSCredentials| opts))
	  (or (not aws:expires) (> (difftime aws:expires) 3600)
	      (and aws/refresh 
		   (begin (lognotice |RefreshToken| aws:key)
		     (aws/refresh #f)))
	      (and err (error |ExpiredAWSCredentials| aws:key))))
      (or (not (getopt opts 'aws:expires))
	  (> (difftime (getopt opts 'aws:expires)) 3600)
	  (and aws/refresh 
	       (begin (lognotice |RefreshToken| aws:key)
		 (aws/refresh opts)))
	  (and err (error |ExpiredAWSCredentials| aws:key)))))

(define (aws/checkok (opts #f)) (aws/ok? opts #t))

(define (aws/set-creds! key secret (token #f) (expires #f) (refresh #f))
  (info%watch "AWS/SET-CREDS!" key secret token expires refresh)
  (set! aws:key key)
  (set! aws:secret secret)
  (set! aws:token token)
  (set! aws:expires expires)
  (set! aws/refresh refresh))

(define (aws/update-creds! opts key secret (token #f) (expires #f) (refresh #f))
  (if (or (not opts) (not (getopt opts 'aws:key)))
      (aws/set-creds! key secret token expires refresh)
      (let ((found (opt/find opts 'aws:key)))
	(store! found 'aws:key key)
	(store! found 'aws:secret secret)
	(when token (store! found 'aws:token token))
	(when expires (store! found 'aws:expires expires))
	(when refresh (store! found 'aws/refresh refresh)))))

(define (aws/creds! arg)
  (if (not arg)
      (begin (aws/set-creds! #f #f #f #f) #f)
      (let* ((spec (if (string? arg)
		       (if (has-prefix arg {"https:" "http:"})
			   (urlcontent arg)
			   (if (has-prefix arg { "/" "~/" "./"})
			       (filecontent arg)
			       arg))
		       arg))
	     (creds (if (string? spec) (jsonparse spec)
			(if (packet? spec) (packet->dtype spec)
			    spec))))
	(aws/set-creds! (try (get creds 'aws:key) (get creds 'accesskeyid))
			(try (->secret (get creds 'aws:secret))
			     (->secret (get creds 'secretaccesskey)))
			(try (get creds 'aws:token)
			     (get creds 'token)
			     #f)
			(try (get creds 'aws:expires)
			     (get creds 'expiration)
			     #f)
			#f)
	creds)))

;;;; Getting JSON templates for AWS APIs

(define (aws/template arg)
  (if (table? arg) arg
      (if (symbol? arg)
	  (if (string? (config arg))
	      (jsonparse (config arg))
	      (let ((found #f)
		    (name (glom (downcase arg) ".json")))
		(dolist (root template-sources)
		  (unless found
		    (when (gp/exists? (gp/mkpath root name))
		      (set! found (gp/mkpath root name)))))
		(if found
		    (jsonparse (gp/fetch found))
		    (irritant ARG |TemplateReference| aws/template
			      "Couldn't resolve template"))))
	  (if (string? arg)
	      (if (textsearch #{"{" "[" "\n"} arg)
		  (jsonparse arg))
	      (if (gp/exists? arg)
		  (jsonparse (gp/fetch arg))
		  (irritant ARG |Template| aws/template))))))

;;;; Extracting AWS error information

(define (aws/error result req)
  (let* ((content (get result '%content))
	 (ctype (try (get result 'content-type) #f))
	 (parsed (parse-error content ctype))
	 (parsetype (try (get parsed 'parsetype) #f))
	 (extra #f))
    (cond ((and (eq? parsetype 'xml) 
		(exists? (xmlget parsed 'INVALIDSIGNATUREEXCEPTION)))
	   (set! extra
		 `#[INVALIDSIGNATUREEXCEPTION 
		    ,(xmlcontent (xmlget parsed 'INVALIDSIGNATUREEXCEPTION) 'message)
		    ACTION ,(getopt req "Action")
		    STRING-TO-SIGN ,(getopt req 'string-to-sign)
		    CREQ ,(getopt req 'creq)
		    DATE ,(getopt req 'date)
		    HEADERS ,(getopt req 'headers)
		    PARAMS ,(getopt req '%params)])))
    (store! result '%content parsed)
    (store! result 'httpstatus (get result 'response))
    (if extra
	(cons* extra result req)
	(cons result req))))

(define (parse-error content type)
  (cond ((and type (search "/xml" type))
	 (xml-error (stringify content)))
	((and type (search "/json" type))
	 (json-error (stringify content)))
	(else (let ((string (stringify content)))
		(if (string-starts-with? string #((spaces*) "<"))
		    (xml-error string)
		    (json-error string))))))

(define (stringify arg)
  (if (string? arg) arg
      (if (packet? arg)
	  (packet->string arg)
	  (stringout arg))))

(define (xml-error xmlstring)
  (onerror
      (let ((err (remove-if string?
			    (xmlparse xmlstring '{data slotify}))))
	(store! (car err) 'parsetype 'xml)
	(car err))
      (lambda (ex)
	(logwarn  (error-condition ex)
	  "Couldn't parse XML error description:\n\t" ex 
	  "\n  " xmlstring))))
(define (json-error jsonstring)
  (onerror
      (let ((err (jsonparse jsonstring)))
	(store! err 'parsetype 'json)
	err)
      (lambda (ex)
	(logwarn  (error-condition ex)
	  "Couldn't parse JSON error description:\n\t" ex 
	  "\n  " jsonstring))))

