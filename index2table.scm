;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'index2table)

(module-export! '{index2table})

(define (index2table index (prefetch #t))
  (when (string? index) (set! index (open-index index)))
  (let* ((keys (getkeys index))
	 (table (make-hashtable (* 2 (choice-size keys)))))
    (when prefetch (prefetch-keys! index keys))
    (do-choices (key keys)
      (store! table key (get index key)))
    table))


