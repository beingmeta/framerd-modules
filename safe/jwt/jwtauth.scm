;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt/auth)

(use-module '{fdweb texttools jwt})
(use-module '{varconfig logger crypto ezrecords})
(define %used_modules '{varconfig ezrecords})

(define-init %loglevel %warn%)
;;(set! %loglevel %debug!)

(module-export! '{auth/getinfo auth/getid 
		  auth/identify! auth/update! 
		  auth/deauthorize! auth/sticky?
		  auth/maketoken})

(module-export! '{jwt/refresh jwt/refreshed})

(define handle-jwt-args (within-module 'jwt handle-jwt-args))

;;;; Constant and configurable variables

(define-init jwt/auth/domain #f)
(varconfig! jwt:auth:domain jwt/auth/domain)
(define-init jwt/auth/defaults #[key "secret" alg "HS256"])
(varconfig! jwt:auth:defaults jwt/auth/defaults)

;; This is the cookie to store the JWT auth token itself
(define-init auth-cookie 'JWT:AUTH)
;; This is the request field for caching the parsee JWT token;
;;  note that _xxx (with the leading underscore) can't be passed
;;  in as a CGI parameter
(define-init auth-cache '_JWT:AUTH)
;; This is where the identity value itself is cached
(define-init identity-cache '_.JWT:AUTH)
(config-def! 'jwt:cookie
	     (lambda (var (val))
	       (if (bound? val)
		   (begin
		     (lognotice |JWT:AUTH| "Set cookie to " val)
		     (set! auth-cookie val)
		     (set! auth-cache (string->symbol (glom "_" val)))
		     (set! identity-cache (string->symbol (glom "_." val))))
		   auth-cookie)))
	     
;; The host name to use for setting the auth cookie
(define-init cookie-host #f)
(varconfig! jwt:cookie:host cookie-host)
;; The path to use for setting the auth cookie
(define-init cookie-path "/")
(varconfig! jwt:cookie:path cookie-path)
;; The lifetime of the cookie if persistent
(define-init cookie-lifetime (* 7 24 3600))
(varconfig! jwt:cookie:lifetime cookie-lifetime)

(define-init jwt/default-refresh 3600) ;; one hour (3600)
(varconfig! jwt:refresh jwt/default-refresh)
;;(set! jwt/default-refresh 30) ;; useful for debugging

;;; This checks the payload for validity.  It is a function
;;;  of the form (fn payload update err)
;;; If update is #t, it may return an updated payload,
;;;  otherwise it returns the payload or #f
;;; Err determines whether or not an error is returned if the 
;;;  check fails
(define-init jwt/checker #f)
(varconfig! jwt:auth:checker jwt/checker)

;; This makes a numeric token identifying a login session
(define (auth/maketoken (length 4) (mult (microtime)))
  (let ((sum 0) (modulus 1/2))
    (dotimes (i length)
      (set! sum (+ (* 256 sum) (random 256)))
      (set! modulus (* modulus 256)))
    (remainder (* sum mult) modulus)))

;;;; Top level auth functions

(define (auth/getinfo (cookie auth-cookie) 
		      (jwtarg (or jwt/auth/domain jwt/auth/defaults)) 
		      (err #f)
		      (cachename))
  (if (eq? cookie auth-cookie)
      (set! cachename auth-cache)
      (set! cachename (string->symbol (glom "_" cookie))))
  (req/get cachename
	   (let* ((bjwt (try (jwt/parse (get-bearer-token) jwtarg) #f))
		  (jwt #f))
	     (if (not bjwt)
		 (set! jwt (or (jwt/parse (or (req/get cookie {}) {}) jwtarg) {}))
		 (set! jwt bjwt))
	     (when (not jwt)
	       (loginfo |JWT/AUTH/getinfo| 
		 "Couldn't get JWT " jwt " from bearer or " cookie))
	     (when jwt 
	       (if (time-earlier? (jwt-expiration jwt))
		   (loginfo |JWT/AUTH/getinfo|
		     "Got JWT " jwt " from " 
		     (if (exists? bjwt) "Bearer authorization" cookie) 
		     "\n    w/payload " (pprint (jwt-payload jwt)))
		   (let ((new (jwt/refresh jwt jwtarg)))
		     (unless (equal? new jwt)
		       (unless bjwt
			 (when new
			   (loginfo |JWT/AUTH/getinfo| 
			     "Refreshed JWT " jwt " to " new " from " 
			     (if (exists? bjwt) "Bearer authorization" cookie) 
			     "\n    w/old payload " (pprint (jwt-payload jwt))
			     "\n    w/new payload " (pprint (jwt-payload new)))
			   (req/set! cachename new)
			   (req/set! cookie (jwt-text new)))
			 (unless new
			   (logwarn |JWT/AUTH/getinfo| 
			     "Failed to refreshed JWT " jwt " from " 
			     (if (exists? bjwt) "Bearer authorization" cookie) 
			     "\n    w/payload " (pprint (jwt-payload jwt)))
			   (req/drop! cachename)
			   (req/drop! cookie)))
		       (set! jwt new)))))
	     (when jwt (req/set! cachename jwt))
	     jwt)))
(define (extract-bearer string)
  (get (text->frame #((bos) (spaces*) "Bearer" (spaces) (label token (rest)))
		    string)
       'token))
(define (get-bearer-token)
  (extract-bearer (req/get 'authorization {})))

(define (auth/getid (cookie auth-cookie) 
		    (jwtarg (or jwt/auth/domain jwt/auth/defaults))
		    (err #f) 
		    (idcache))
  (if (eq? cookie auth-cookie)
      (set! idcache identity-cache)
      (set! idcache (string->symbol (glom "__" cookie))))
  (req/get idcache
	   (let* ((jwt (auth/getinfo cookie jwtarg err))
		  (id (tryif jwt (parse-arg (jwt/get jwt 'sub)))))
	     (when (and (exists? jwt) (fail? id))
	       (logwarn |JWT/AUTH/noid| "Couldn't get id (sub) from " jwt))
	     (when (exists? id)
	       (loginfo |JWT/AUTH/getid| "Got id " id " from " jwt)
	       (req/set! idcache id))
	     (try id #f))))

(define (auth/sticky? (arg auth-cookie))
  (if (jwt? arg)
      (try (jwt/get arg 'sticky) #f)
      (try (jwt/get (auth/getinfo arg) 'sticky) #f)))

;;; Refreshing tokens

(define (jwt/refresh jwt (opts) (key) (alg) (checker) (issuer))
  "Refresh a JWT if needed or returns the JWT as is"
  (handle-jwt-args)
  (default! checker (getopt opts 'checker jwt/checker))
  (debug%watch "JWT/REFRESH" 
    jwt "EXPIRES" (jwt-expiration jwt) opts key alg checker issuer)
  (and (or (jwt-valid jwt) (jwt/valid? jwt key alg issuer))
       (or (and (jwt-expiration jwt) (time-earlier? (jwt-expiration jwt)) jwt)
	   (not (jwt-expiration jwt))
	   (if (jwt-expiration jwt)
	       (let ((payload (if checker 
				  (checker (jwt-payload jwt) #t #f)
				  (deep-copy (jwt-payload jwt))))
		     (refresh (getopt opts 'refresh jwt/default-refresh))
		     (new #f))
		 (when payload 
		   (if refresh
		       (store! payload 'exp (time+ refresh))
		       (drop! payload 'exp))
		   (set! new (jwt/make payload opts key alg checker issuer)))
		 (loginfo |JWT/refresh| "Refreshed JWT " jwt " ==> " new)
		 new)
	       jwt))))

(define (jwt/refreshed jwt (opts) (key) (alg) (checker) (issuer))
  "Refresh a JWT if needed or fails otherwise"
  (and (or (jwt-valid jwt) (jwt/check jwt))
       (if (or (and (jwt-expiration jwt) (time-earlier? (jwt-expiration jwt)))
	       (and (not (jwt-expiration jwt)) (not checker)))
	   (fail)
	   (let ((payload (if checker 
			      (checker (jwt-payload jwt) #t #f)
			      (deep-copy (jwt-payload jwt))))
		 (refresh (getopt opts 'refresh jwt/default-refresh)))
	     (when payload
	       (if refresh
		   (store! payload 'exp (time+ refresh))
		   (drop! payload 'exp)))
	     (and payload 
		  (jwt/make payload opts key alg issuer))))))

;;;; Authorize/deauthorize API

(define (auth/identify! identity (cookie auth-cookie) 
			(jwtarg (or jwt/auth/domain jwt/auth/defaults))
			(payload #t)
			(opts)
			(checker))
  (cond ((number? payload) 
	 (set! payload `#[sticky ,payload]))
	((eq? payload #t)
	 (set! payload `#[sticky ,cookie-lifetime]))
	((not payload) (set! payload `#[]))
	((not (table? payload)) (set! payload `#[])))
  (default! opts
    (cond ((table? jwtarg) jwtarg)
	  ((symbol? jwtarg) (get jwt/configs jwtarg))
	  ((jwt? jwtarg)
	   (get jwt/configs (jwt-domain jwtarg)))
	  (else #[])))
  (default! checker (getopt opts 'checker jwt/checker))
  (when (jwt? identity)
    (set! payload (jwt-payload identity))
    (set! identity (get payload 'sub)))
  (and identity
       (let* ((payload (frame-update payload 'sub identity))
	      (checked (if checker 
			   (checker payload 'init #t)
			   (frame-update payload 'token (auth/maketoken))))
	      (jwt (jwt/make checked jwtarg))
	      (text (and jwt (jwt-text jwt))))
	 (lognotice |JWT/AUTH/identify!|
	   "Identity=" identity " in " cookie 
	   " signed by " jwtarg " w/payload " 
	   "\n" (pprint checked))
	 (detail%watch "AUTH/IDENTIFY!" 
	   identity session expires payload jwt (auth->string auth))
	 (unless text
	   (logwarn |JWT/MAKE/Failed| 
	     "Couldn't sign JWT for " identity " with arg=" jwtarg
	     ", payload=" payload ", and jwt=" jwt))
	 (when text
	   (let* ((info (and (table? cookie) cookie))
		  (name (if info
			    (try (get info 'name) auth-cookie)
			    cookie))
		  (domain (if info 
			      (try (get info 'domain) cookie-host)
			      cookie-host))
		  (path (if info 
			    (try (get info 'path) cookie-path)
			    cookie-path)))
	     (req/set! name text)
	     (req/set! (string->symbol (glom "_" name)) jwt)
	     (req/set! (string->symbol (glom "__" name)) identity)
	     (if (test payload 'sticky)
		 (set-cookie! name text domain path
			      (time+ (get payload 'sticky)) #t)
		 (set-cookie! name text domain path #f #t))))
	 identity)))

(define (auth/deauthorize! (cookie auth-cookie))
  (when (req/test cookie)
    (set-cookie! cookie "expired" cookie-host cookie-path
		 (time- (* 7 24 3600))
		 #t)))

(define (auth/update! (cookie auth-cookie) 
		      (jwtarg (or jwt/auth/domain jwt/auth/defaults)))
  (when (req/test 'cookie)
    (let ((jwt (jwt/refreshed (auth/getinfo cookie jwtarg))))
      (when (and (exists? jwt) jwt)
	(lognotice |JWT/AUTH/update| "Updating JWT authorization " jwt)
	(set-cookie! cookie (jwt-text jwt) cookie-host cookie-path
		     (and (jwt/get jwt 'sticky)
			  (time+ (jwt/get jwt 'sticky)))
		     #t)))))
