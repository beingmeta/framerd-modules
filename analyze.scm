;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2018 beingmeta, inc.  All rights reserved.

(in-module 'analyze)

(module-export! '{get->index 
		  table-ambig table-ambig?
		  table-unique table-unique?})

(defambda (get->index args slot)
  (let* ((oids (pickoids args))
	 (other-args (difference args oids))
	 (pools (pick other-args pool?))
	 (tables (pick other-args table?)))
    (do-choices (pool pools) (set+! oids (pool-elts pool)))
    (prefetch-oids! oids)
    (for-choices slot
      (let ((inverse (make-hashtable)))
	(do-choices (oid oids) (add! inverse (get oid slot) oid))
	(do-choices (table tables) (add! inverse (get table slot) table))
	inverse))))

(define (table-ambig table (keys) (ambig {}))
  (default! keys (getkeys table))
  (do-choices (key keys)
    (when (ambiguous? (get table key)) (set+! ambig key)))
  ambig)

(define (table-ambig? table (keys))
  (default! keys (getkeys table))
  (exists? (table-ambig table (qc keys))))

(define (table-unique table (keys) (unique {}))
  (default! keys (getkeys table))
  (do-choices (key keys)
    (when (singleton? (get table key)) (set+! unique key)))
  unique)

(define (table-unique? table (keys))
  (default! keys (getkeys table))
  (fail? (table-ambig table keys)))
