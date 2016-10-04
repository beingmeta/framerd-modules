;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/pools)

(use-module '{mongodb})

(module-export! '{mgo/pool mgo/poolfetch
		  mgo/store! mgo/drop! mgo/add!})

(define-init pool-table (make-hashtable))

(define (fetchfn oid/s collection)
  (if (vector? oid/s)
      (mongodb/find collection #[_id #[$oneof oid/s]])
      (mongodb/get collection oid/s)))

(define (mgo/poolfetch oid/s collection) (fetchfn oid/s collection))

(define (mgo/pool collection base cap)
  (try (get pool-table (vector (mongodb/spec collection)
			       (collection/name collection)))
       (let ((pool (make-extpool (collection/name collection) base cap
				 mgo/poolfetch #f #f #f
				 collection #t)))
	 (store! pool-table pool collection)
	 (store! pool-table (vector (mongodb/spec collection)
				    (collection/name collection)) 
		 pool)
	 pool)))

(defambda (mgo/store! oid slotid values (pool) (collection))
  (set! pool (getpool oid))
  (set! collection (get pool-table pool))
  (if (or (fail? pool) (not pool))
      (irritant oid |No pool| mgo/store!)
      (if (fail? collection)
	  (irritant pool |Not A MongoDB pool| mgo/store!)
	  (mongodb/update! collection `#[_id ,oid]
			   #[$set #[,slotid ,values]]))))

(defambda (mgo/add! oid slotid values (pool) (collection))
  (set! pool (getpool oid))
  (set! collection (get pool-table pool))
  (if (or (fail? pool) (not pool))
      (irritant oid |No pool| mgo/store!)
      (if (fail? collection)
	  (irritant pool |Not A MongoDB pool| mgo/store!)
	  (mongodb/update! collection `#[_id ,oid]
			   #[$addToSet #[,slotid ,values]]))))

(defambda (mgo/drop! oid slotid (values) (pool) (collection))
  (set! pool (getpool oid))
  (set! collection (get pool-table pool))
  (if (or (fail? pool) (not pool))
      (irritant oid |No pool| mgo/store!)
      (if (fail? collection)
	  (irritant pool |Not A MongoDB pool| mgo/store!)
	  (if (bound? values)
	      (mongodb/update! collection `#[_id ,oid]
			       #[$pull #[,slotid ,values]])
	      (mongodb/update! collection `#[_id ,oid]
			       #[$uset #[,slotid 1]])))))
