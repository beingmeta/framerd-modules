;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'aws/v2)

;;; Accessing Amazon Simple DB

(use-module '{aws fdweb texttools logger fdweb varconfig jsonout rulesets})

(define-init %loglevel %notice%)

(module-export! 
 '{aws/v2/sigstring aws/v2/sigstring/sign aws/v2/signature
   aws/v2/prepare! aws/v2/op})

(define uri-prefix-pat
  #((bos)
    (label scheme {"http:" "https:"}) "/"
    (label host (not> {"/" ":"}))
    (opt #(":" (label port (isdigit+) #t)))
    (opt "/")))

(define (uriport uri)
  (let* ((matches (text->frames uri-prefix-pat uri))
	 (default-port (if (test matches 'scheme "http") 80
			   (if (test matches 'scheme "https")
			       443
			       #f))))
    (try (difference (get matches 'port) default-port)
	 #f)))

(define (aws/v2/sigstring method uri args (amp #f))
  (stringout method "\n" 
    (urihost uri) (if (uriport uri) (printout ":" (uriport uri))) "\n"
    (dolist (seg (segment (uripath uri) "/"))
      (printout (uriencode seg) "/"))
    "\n"
    (doseq (key (lexsorted (getkeys args)))
      (do-choices (v (get args key))
	(printout (if amp "&" (set! amp #t))
	  (uriencode key) "=" (printout (uriencode v)))))))

(define (aws/v2/sigstring/sign sigstring secret)
  (hmac-sha1 secret sigstring))

(define (aws/v2/signature secret method uri args)
  (hmac-sha1 secret (aws/v2/sigstring method uri args)))

(define (aws/v2/prepare! req method uri args (timestamp))
  (default! timestamp (getopt req 'timestamp (gmtimestamp 'seconds)))
  (store! args "AWSAccessKeyId" (getopt req 'aws:key aws:key))
  (store! args "SignatureVersion" 2)
  (store! args "SignatureMethod" "HmacSHA256")
  (store! args "Timestamp" 
	  (if (string? timestamp) timestamp
	      (get timestamp 'iso)))
  (store! args "Version" (getopt req 'version "2014-06-15"))
  (let* ((secret (getopt req 'aws:secret aws:secret))
	 (sigstring (aws/v2/sigstring method uri args))
	 (sig (aws/v2/sigstring/sign sigstring secret)))
    (store! req 'sigstring sigstring)
    (store! args "Signature" (downcase (packet->base16 sig))))
  (when (getopt req 'token)
    (store! args "SecurityToken" (getopt req 'token)))
  (store! req 'args args))

(define (aws/v2/op req method uri args)
  (aws/v2/prepare! req method uri args)
  (cons (urlget (scripturl+ uri args) #[verbose #t]) req))


