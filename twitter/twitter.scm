;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'twitter)

(use-module '{fdweb texttools reflection varconfig parsetime logger})
(use-module '{xhtml xhtml/auth})

(module-export! '{->tweet twitter/user twitter/tag twitter/url})

(define-init %loglevel %notice%)

(define (->tweet result (pool #f))
  (let ((f (frame-create pool
	     'id (get result 'id) 
	     'created (parsetime (get result 'created_at))
	     'text (get result 'text)))
	(hashtags (get (get result 'entities) 'hashtags)))
    (store! f 'hashtags (twitter/tag hashtags))
    (store! f '%hashtags hashtags)
    (store! f 'urls (twitter/url (get (get result 'entities) 'urls)))
    (store! f 'user (twitter/user (get result 'user)))
    (do-choices (slot (difference (getkeys result) '{ID_STR ENTITIES USER}))
      (let ((v (get result slot)))
	(unless (or (fail? v) (not v))
	  (if (and (table? v) (test v 'id) (test v 'created_at) (test v 'text))
	      (store! f slot (->tweet v pool))
	      (store! f slot v)))))
    f))

(define (twitter/user v)
  (let ((result (frame-create #f
		  'id (get v 'id) 'name (get v 'name)
		  'handle (get v 'screen_name) 'description (get v 'description)
		  'url (twitter/url (get (get (get v 'entities) 'url) 'urls))
		  'registered (parsetime (get v 'created_at)))))
    (do-choices (slot (difference (getkeys v) 
				  '{ID NAME SCREEN_NAME 
				    ID_STR ENTITIES USER}))
      (let ((v (get result slot)))
	(cond ((or (fail? v) (not v)))
	      ((number? v) (store! result slot v)))))))

(define (twitter/tag v) 
  (if (vector? v) (twitter/tag (elts v))
      (try (get v 'text) v)))

(define (twitter/url url)
  (if (vector? url)
      (twitter/url (elts url))
      (frame-create #f 
	'url (try (get url 'expanded_url) (get url 'url))
	'tiny (get url 'url)
	'display (get url 'display_url)
	'textspan (get url 'indices))))







