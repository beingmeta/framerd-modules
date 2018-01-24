;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'transformers)

(module-export! '{index2table copy-index2table})

(define (index2table index (prefetch #t))
  (when (string? index) (set! index (open-index index)))
  (let* ((keys (getkeys index))
	 (table (make-hashtable (* 2 (choice-size keys)))))
    (when prefetch (prefetch-keys! index keys))
    (do-choices (key keys)
      (store! table key (get index key)))
    table))

(define (copy-index2table indexfile dtypefile)
  (let* ((index (open-index indexfile))
	 (table (index2table index)))
    (if (has-suffix dtypefile {".ztype"})
	(dtype->zfile table dtypefile)
	(dtype->file table dtypefile))
    table))


  




