#!/usr/bin/env fdexec
;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(define brico-dir (get-component "new/"))
(config! 'bricosource brico-dir)

(define local-wikidata-dir (get-component "wikidata/"))
(config! 'wikidata:root local-wikidata-dir)

(use-module '{logger varconfig stringfmts optimize})
(use-module '{fdweb libarchive})
(use-module '{storage/flex storage/typeindex})
(use-module '{brico brico/indexing brico/build/wikidata})

(define %loglevel %info%)

(config! 'dbloglevel %info%)
(config! 'cachelevel 2)
(define wikidata-input-file
  (get-component "wikisrc/latest-all.json.bz2"))
(indexctl meta.index 'readonly #f)
(poolctl brico.pool 'readonly #f)

;;; Converting dump data

(define (wikid/reader (file wikidata-input-file) (lines 0) (pos #f))
  (if (has-suffix file ".bz2")
      (let ((stream (archive/open file 0))
	    (count 0)
	    (eof #f))
	;; Ignore leading [
	(getline stream "[")
	(dotimes (i lines) (getline stream))
	(set! count lines)
	(slambda ((cmd #f))
	  (cond ((and eof (not cmd)) #f)
		((not cmd)
		 (set! count (1+ count))
		 (try (getline stream)
		      (begin (set! eof #t) #f)))
		((not cmd) #f)
		((eq? cmd 'count) count)
		((eq? cmd 'eof?) eof)
		((eq? cmd 'pos) #f)
		(else 'huh))))
      (let ((stream (open-input-file file))
	    (count 0)
	    (eof #f))
	;; Ignore leading [
	(if pos
	    (setpos stream pos)
	    (begin
	      (getline stream "[")
	      (dotimes (i lines) (getline stream))))
	(set! count lines)
	(slambda ((cmd #f))
	  (cond ((and eof (not cmd)) #f)
		((not cmd)
		 (set! count (1+ count))
		 (try (getline stream)  #f))
		((eq? cmd 'count) count)
		((eq? cmd 'pos) (getpos stream))
		((eq? cmd 'eof?) (>= (getpos stream) (endpos stream)))
		(else 'huh))))))

;;;; Cycles

(define-init base-wikid-load (pool-load wikid.pool))
(define-init base-brico-load (pool-load brico.pool))
(define-init session-started #f)

(define (wikid/cycle r (opts #f))
  (let ((started (elapsed-time))
	(start-brico-load (pool-load brico.pool))
	(start-wikid-load (pool-load wikid.pool)))
    (unless session-started (set! session-started started))
    (wikid/readn r wikid.index opts)
    (wikid/commit)
    (swapout)
    (lognotice |Cycle| 
      "Finished one cycle in " (secs->string (elapsed-time started)) 
      ", creating "
      ($count (- (pool-load wikid.pool) start-wikid-load) "new item")
      " and "
      ($count (- (pool-load brico.pool) start-brico-load)
	      "new property" "new properties"))
    (lognotice |Overall| 
      "Created " ($count (- (pool-load wikid.pool) base-wikid-load)
			 "new item")
      " and "
      ($count (- (pool-load brico.pool) base-brico-load)
	      "new property" "new properties")
      " in " (secs->string (elapsed-time session-started)))))

(define (main (input-file wikidata-input-file)
	      (lines (config 'LINES 0)) (pos #f)
	      (maxlines (config 'maxlines)))
  (let ((r (wikid/reader input-file lines pos))
	(started (elapsed-time))
	(done #f))
    (until (or done (and maxlines (> (- (r 'count) lines) maxlines)))
      (wikid/cycle r)
      (lognotice |Progress|
	"Read " ($num (- (r 'count) lines)) "/" ($num (r 'count)) " lines "
	"from " input-file " in " (secs->string (elapsed-time started))
	(when (r 'pos)
	  (let ((size (file-size input-file))
		(pos (r 'pos)))
	    (printout ", or " ($bytes pos) " "
	      "(" (show% pos size) ")"))))
      (when (r 'eof?)
	(logwarn |FileDone| "Finished reading " input-file)
	(set! done #t)))
    (wikid/commit)
    (unless (r 'eof?)
      (if (r 'pos)
	  (chain input-file (r 'count) (r 'pos))
	  (chain input-file (r 'count))))))

(when (config 'optimize #t)
  (optimize! '{brico brico/indexing brico/wikidata
	       storage/flex storage/flexpools})
  (optimize!))
