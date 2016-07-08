;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'aws/ec2data)

(use-module '{aws aws/v4 fdweb texttools mimetable regex logctl
	      ezrecords rulesets logger varconfig})
(define %used_modules '{aws varconfig ezrecords rulesets})

(define %loglevel %warn%)

(module-export! '{ec2/data ec2/getrole})

(define ec2-instance-data-root "http://169.254.169.254/")

(define (parse-newline-list response)
  (elts (segment (get response '%content) "\n")))

(define prop-info
  `#[metadata ("/meta-data/" . ,parse-newline-list)
     ami ("/meta-data/ami-id" . ,string->lisp)
     id ("/meta-data/instance-id" . ,string->lisp)
     launch-index ("/meta-data/ami-launch-index" . ,string->lisp)])

(define metadata-properties
  {
   "mac"
   "ami-id"
   "profile"
   "hostname"
   "metrics/"
   "services/domain"
   "iam/info"
   "local-ipv4"
   "placement/availability-zone"
   "instance-id"
   "public-ipv4"
   "instance-type"
   "local-hostname"
   "reservation-id"
   "instance-action"
   "public-hostname"
   "security-groups"
   "ami-launch-index"
   "ami-manifest-path"
   "block-device-mapping/"
   })

(define ec2-data-endpoints
  {"/user-data" (glom "/meta-data/" metadata-properties)})

(do-choices (endpoint ec2-data-endpoints)
  (let ((base (basename (strip-suffix endpoint "/"))))
    (store! prop-info base endpoint)
    (store! prop-info (string->symbol (upcase base)) endpoint)))

(define (ec2/data (prop #f) (version "latest") (error #f))
  (if (not version) (set! version "latest"))
  (if (not prop)
      (let ((data (frame-create #f)))
	(for-choices (endpoint ec2-data-endpoints)
	  (store! data 
		  (string->symbol
		   (upcase
		    (basename
		     (if (has-suffix endpoint "/") 
			 (slice endpoint 0 -1)
			 endpoint))))
		  (ec2/data endpoint version #f)))
	data)
      (let* ((propinfo
	      (try (get prop-info prop)
		   (if (string? prop) prop
		       (tryif prop
			 (irritant prop |UnknownProperty| ec2/data)))))
	     (path (tryif (exists? propinfo)
		     (if (string? propinfo) propinfo
			 (if (pair? propinfo) (car propinfo)
			     (get propinfo 'path)))))
	     (handler (if (pair? propinfo) (cdr propinfo)
			  (tryif (table? propinfo) (get propinfo 'handler))))
	     (url (glom ec2-instance-data-root version "/" path))
	     (response (urlget url))
	     (status (get response 'response))
	     (content (get response '%content))
	     (type (get response 'content-type)))
	(debug%watch "EC2/DATA" prop propinfo path handler url type status)
	(detail%watch "EC2/DATA" response)
	(if (fail? url) #f
	    (if (= status 200)
		(cond ((and (exists? handler) handler) (handler response))
		      ((equal? type "text/plain")
		       (if (has-suffix path "/")
			   (parse-newline-list response)
			   (if (has-prefix content {"{" "["})
			       (jsonparse content)
			       content)))
		      ((not (string? content)) content)
		      ((has-prefix content {"{" "["}) (jsonparse content))
		      (else response))
		(if error
		    (irritant response |BadEC2DataResponse| ec2/data)
		    (begin (logwarn |Instance data failed| url " status " status)
		      #f)))))))

(define (ec2/getrole)
  (let* ((data (ec2/data "meta-data/iam/info"))
	 (parsed (if (string? data) (jsonparse data) data)))
    (try (basename (get parsed 'instanceprofilearn))
	 #f)))
