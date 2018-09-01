#!/usr/bin/env fdexec
;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(define brico-dir (abspath "forwikid"))
(config! 'bricosource brico-dir)

(define local-wikidata-dir (abspath "wikidata/"))
(config! 'wikidata:root local-wikidata-dir)

(use-module '{logger varconfig stringfmts optimize})
(use-module '{fdweb libarchive})
(use-module '{storage/flex storage/typeindex})
(use-module '{brico brico/indexing brico/build/wikidata})

(define %loglevel %notice%)

(config! 'dbloglevel %info%)
(config! 'log:threadid #t)
(config! 'cachelevel 2)
(define wikidata-input-file
  (abspath "wikisrc/latest-all.json.bz2"))
(indexctl meta.index 'readonly #f)
(poolctl brico.pool 'readonly #f)

(define dochain #t)
(varconfig! chain dochain config:boolean)

;;; Reading import state

(define import-state #f)

;;; Converting exported wikidata

(define (wikid/reader file (lines 0) (pos #f))
  (unless (number? lines) (set! lines 0))
  (if (has-suffix file ".bz2")
      (let ((stream (archive/open file 0))
	    (count 0)
	    (eof #f))
	;; Ignore leading [
	(getline stream "[")
	;; And processed lines
	(logwarn |SkippingLines| "Skipping " ($count lines "line") " of input")
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
	(if pos
	    (setpos stream pos)
	    (begin
	      ;; Ignore leading [
	      (getline stream "[")
	      ;; And processed lines
	      (logwarn |SkippingLines| "Skipping " ($count lines "line") " of input")
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
    (wikid/readn r wikidata.index opts)
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

(define (main (input-file #f)
	      (lines (config 'LINES 0)) (pos #f)
	      (maxlines (config 'maxlines)))
  (let* ((state (if (file-exists? (mkpath local-wikidata-dir "import.state"))
		    (file->dtype (mkpath local-wikidata-dir "import.state"))
		    `#[]))
	 (file (or input-file (getopt state 'file wikidata-input-file)))
	 (lines (getopt state 'lines (config 'LINES 0)))
	 (pos (getopt state 'pos (config 'STARTPOS 0)))
	 (started (elapsed-time))
	 (done #f)
	 (r #f))
    (info%watch "MAIN" lines pos r started done)
    (set! r (wikid/reader file lines pos))
    (lognotice |Starting| "Reading wikidata")
    (until (or done (and maxlines (> (- (r 'count) lines) maxlines))
	       (file-exists? (mkpath local-wikidata-dir "stop")))
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
	(set! done #t))
      (wikid/commit)
      (store! state 'file file)
      (store! state 'lines (r 'count))
      (store! state 'pos (r 'pos))
      (store! state 'elapsed
	      (+ (try (get state 'elapsed) 0)
		 (elapsed-time started)))
      (dtype->file state (mkpath local-wikidata-dir "import.state")))
    (unless (or (not dochain) (r 'eof?)
		(file-exists? (mkpath local-wikidata-dir "stop")))
      (if (r 'pos)
	  (chain input-file (r 'count) (r 'pos))
	  (chain input-file (r 'count))))))

(when (config 'optimize #t)
  (optimize! '{brico brico/indexing brico/build/wikidata
	       storage/flex storage/flexpools})
  (optimize!))
