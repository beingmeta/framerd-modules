;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'twitter)

(use-module '{fdweb texttools reflection varconfig parsetime logger})
(use-module '{xhtml xhtml/auth oauth xconfig})

(module-export! '{twitter/creds})

(define-init %loglevel %notice%)

(define init-creds 'twitter20)
(define twitter-creds #f)

(define (twitter/creds (init init-creds))
  (or (and twitter-creds (not init))
      (let ((creds (oauth/getclient init)))
	(set! twitter-creds creds)
	creds)))











