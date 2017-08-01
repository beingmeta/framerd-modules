;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger})
(define %used_modules 'ezrecords)

(module-export!
 '{fifo/make fifo/close
   fifo/pop fifo/remove!
   fifo/push! fifo/push/n! fifo/jump! fifo/pause!  
   fifo/loop fifo/queued fifo/load fifo/paused?})

(module-export!
 '{make-fifo
   fifo-push fifo-pop fifo-jump fifo-loop fifo-queued close-fifo
   fifo-load fifo-live? fifo-waiting fifo/waiting
   fifo/idle? fifo/set-debug!})

(define-init %loglevel %warn%)

;;;; Implementation

(define (fifo->string fifo)
  (stringout "#<FIFO "
    (- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) " "
    (or (fifo-name fifo) (glom "0x" (number->string (hashptr fifo) 16))) 
    (if (not (fifo-live? fifo)) " (dead)")
    (if (fifo-debug fifo) " (debug)")
    ">"))

(defrecord (fifo MUTABLE OPAQUE `(stringfn . fifo->string))
  name state queue items start end live? (pause #f)
  (waiting 0) (running 0) (debug #f))

(define (fifo/make (name #f) (size #f) (data #f) (queue))
  (%watch "FIFO/MAKE" name size data)
  (cond ((and (number? name) (not size))
	 (set! size name) (set! name #f))
	((and (vector? name) (not data))
	 (logwarn |InitialzingFIFOData| name)
	 (set! data name)
	 (set! size (length data))
	 (set! name #f))
	((and (vector? size) (not data))
	 (set! data size)
	 (set! size (length data))))
  (when (and (not size) (not data)) (set! size 64))
  (%watch "FIFO/MAKE/later" name size data)
  (cond ((and (vector? data) (= (length data) size))
	 (set! queue data))
	((vector? data)
	 (set! queue (make-vector size))
	 (doseq (elt data i) (vector-set! queue i elt)))
	(else (set! queue (make-vector size #f))))
  (cons-fifo name (make-condvar) queue (make-hashset)
	     0
	     (if data (compact-queue queue) 0)
	     #t))
(define (make-fifo (size 64)) (fifo/make size))

(define (compact-queue queue)
  (let ((write 0) (len (length queue)))
    (doseq (item queue read)
      (cond ((not item))
	    ((= write read) (set! write (1+ write)))
	    (else (vector-set! queue write item)
		  (set! write (1+ write)))))
    (dotimes (i (- len write))
      (vector-set! queue (+ write i) #f))
    write))

(defambda (check-paused fifo flags)
  (while (overlaps? (fifo-pause fifo) flags)
    (condvar-wait (fifo-state fifo))))

(define (fifo/push! fifo item (broadcast #f))
  (unwind-protect
      (begin (if (fifo-debug fifo)
		 (always%watch "FIFO/PUSH!" item broadcast fifo)
		 (debug%watch "FIFO/PUSH!" item broadcast fifo))
	(condvar-lock (fifo-state fifo))
	(check-paused fifo '{write readwrite})
	(if (hashset-get (fifo-items fifo) item)
	    (if (fifo-debug fifo)
		(always%watch "FIFO/PUSH!/REDUNDANT" item fifo)
		(debug%watch "FIFO/PUSH!/REDUNDANT" item fifo))
	    (let ((vec (fifo-queue fifo))
		  (start (fifo-start fifo))
		  (end (fifo-end fifo)))
	      (if (fifo-debug fifo)
		  (always%watch "FIFO/PUSH!/INSERT" item fifo)
		  (debug%watch "FIFO/PUSH!/INSERT" item fifo))
	      (cond ((< end (length vec))
		     (vector-set! vec end item)
		     (set-fifo-end! fifo (1+ end)))
		    ((<= (- end start) (-1+ (length vec)))
		     ;; Move the queue to the start of the vector
		     (dotimes (i (- end start))
		       (vector-set! vec i (elt vec (+ start i))))
		     (let ((zerostart (- end start)))
		       (dotimes (i (- (length vec) zerostart))
			 (vector-set! vec (+ zerostart i) #f)))
		     (set-fifo-start! fifo 0)
		     (vector-set! vec (- end start) item)
		     (set-fifo-end! fifo (1+ (- end start))))
		    (else
		     (let ((newvec (make-vector (* 2 (length vec)))))
		       (if (fifo-debug fifo)
			   (always%watch "FIFO/PUSH!/GROW" fifo)
			   (debug%watch "FIFO/PUSH!/GROW" fifo))
		       (debug%watch "FIFO/PUSH!/GROW" fifo)
		       (dotimes (i (- end start))
			 (vector-set! newvec i (elt vec (+ start i))))
		       (set-fifo-queue! fifo newvec)
		       (set-fifo-start! fifo 0)
		       (vector-set! newvec (- end start) item)
		       (set-fifo-end! fifo (1+ (- end start))))))))
	(condvar-signal (fifo-state fifo) broadcast))
    (condvar-unlock (fifo-state fifo))))
(define (fifo-push fifo item (broadcast #f)) (fifo/push! fifo item broadcast))

(define (fifo/push/n! fifo items (broadcast #f))
  (unwind-protect
      (begin (if (fifo-debug fifo)
		 (always%watch "FIFO/PUSH/N!" "N" (length items) broadcast fifo)
		 (debug%watch "FIFO/PUSH/N!" "N" (length items) broadcast fifo))
	(condvar-lock (fifo-state fifo))
	(while (overlaps? (fifo-pause fifo) '{write readwrite})
	  (condvar-wait (fifo-state fifo)))
	(let ((vec (fifo-queue fifo))
	      (start (fifo-start fifo))
	      (end (fifo-end fifo))
	      (add (length items)))
	  (cond ((< (+ end add) (length vec))
		 (doseq (item items i)
		   (vector-set! vec (+ end i) item))
		 (set-fifo-end! fifo (+ end add)))
		((< (+ add (- end start)) (length vec))
		 ;; Move the queue to the start of the vector
		 (dotimes (i (- end start))
		   (vector-set! vec i (elt vec (+ start i))))
		 (let ((zerostart (- end start)))
		   (dotimes (i (- (length vec) zerostart))
		     (vector-set! vec (+ zerostart i) #f)))
		 (set-fifo-start! fifo 0)
		 (set! end (- end start))
		 (doseq (item items i)
		   (vector-set! vec (+ end i) item))
		 (set-fifo-end! fifo (+ end add)))
		(else
		 (let* ((needed (+ (- end start) add 1))
			(newlen (getlen needed (length vec)))
			(newvec (make-vector newlen)))
		   (if (fifo-debug fifo)
		       (always%watch "FIFO/PUSH!/GROW" fifo)
		       (debug%watch "FIFO/PUSH!/GROW" fifo))
		   (debug%watch "FIFO/PUSH!/GROW" fifo)
		   (dotimes (i (- end start))
		     (vector-set! newvec i (elt vec (+ start i))))
		   (set-fifo-queue! fifo newvec)
		   (set-fifo-start! fifo 0)
		   (set! end (- end start))
		   (doseq (item items i)
		     (vector-set! vec (+ end i) item))
		   (set-fifo-end! fifo (+ end add))))))
	(condvar-signal (fifo-state fifo) broadcast))
    (condvar-unlock (fifo-state fifo))))

(define (getlen needed current)
  (if (< needed current) 
      current
      (let ((trylen (* 2 current)))
	(while (> needed trylen) (set! trylen (* 2 trylen)))
	trylen)))

(define (fifo-waiting! fifo flag)
  "Declare that a thread is either waiting on the FIFO"
  (if (fifo-debug fifo)
      (always%watch "FIFO-WAITING!" flag (fifo-waiting fifo) fifo)
      (debug%watch "FIFO-WAITING!" flag (fifo-waiting fifo) fifo))
  (set-fifo-waiting! fifo (+ (if flag 1 -1) (fifo-waiting fifo)))
  (set-fifo-running! fifo (+ (if flag -1 1) (fifo-running fifo)))
  (condvar-signal (fifo-state fifo) #t)
  (fifo-waiting fifo))

(define (fifo/pop fifo (maxcount 1))
  (if (fifo-debug fifo)
      (always%watch "FIFO/POP" fifo)
      (debug%watch "FIFO/POP" fifo))
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-state fifo))
	    ;; Assert that we're waiting
	    (fifo-waiting! fifo #t)
	    ;; Wait if it is paused
	    (check-paused fifo '{read readwrite})
	    ;; Wait for data
	    (while (and (fifo-live? fifo)
			(= (fifo-start fifo) (fifo-end fifo))
			(not (overlaps? (fifo-pause fifo) '{read readwrite})))
	      (condvar-wait (fifo-state fifo)))
	    (fifo-waiting! fifo #f)
	    (check-paused fifo '{read readwrite})
	    ;; If it's still alive, do the pop
	    ;; Wait if it is paused
	    (if (fifo-live? fifo) 
		(let* ((vec (fifo-queue fifo))
		       (start (fifo-start fifo))
		       (end (fifo-end fifo))
		       (count (min maxcount (- end start)))
		       (items (elts vec start (+ start count))))
		  (if (fifo-debug fifo)
		      (always%watch "FIFO/POP/ITEM" start end item fifo)
		      (debug%watch "FIFO/POP/ITEM" start end item fifo))
		  ;; Replace the item with false
		  (dotimes (i count)
		    (vector-set! vec (+ start i) #f))
		  ;; Advance the start pointer
		  (set-fifo-start! fifo (+ start count))
		  (when (= (fifo-start fifo) (fifo-end fifo))
		    ;; If we're empty, move the pointers back
		    (set-fifo-start! fifo 0)
		    (set-fifo-end! fifo 0))
		  (hashset-drop! (fifo-items fifo) items)
		  items)
		(fail)))
	(condvar-unlock (fifo-state fifo)))
      (fail)))
(define (fifo-pop fifo) (fifo/pop fifo))

(define (fifo/remove! fifo item)
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-state fifo))
	    (if (and (fifo-live? fifo)
		     (< (fifo-start fifo) (fifo-end fifo)))
		(let* ((vec (fifo-queue fifo))
		       (start (fifo-start fifo))
		       (end (fifo-end fifo))
		       (pos (position item vec start end)))
		  (if pos
		      (let ((queued (elt vec pos)))
			;; Move everything else down
			(dotimes (i (- end pos))
			  (vector-set! vec (+ pos i) (elt vec (+ pos i 1))))
			(vector-set! vec end #f)
			(set-fifo-end! fifo (-1+ end))
			(hashset-drop! (fifo-items fifo) item)
			queued)
		      (fail)))
		(fail)))
	(condvar-unlock (fifo-state fifo)))
      (fail)))
(define (fifo-jump fifo item) (fifo/remove! fifo item))

(define (fifo/loop fifo handler)
  (let ((result #t))
    (while (and (exists? result) result)
      (set! result (handler (fifo-pop fifo))))))
(define (fifo-loop fifo handler) (fifo/loop fifo handler))

(define (fifo/close fifo (broadcast #t) (result #f))
  (unwind-protect
      (begin
	(condvar-lock (fifo-state fifo))
	(set! result
	      (slice (fifo-queue fifo)
		     (fifo-start fifo) (fifo-end fifo)))
	(set-fifo-live?! fifo #f)
	result)
    (begin
      (unless broadcast (fifo-state fifo))
      (when broadcast
	(condvar-signal (fifo-state fifo) #t)
	(condvar-unlock (fifo-state fifo))))))
(define (close-fifo fifo (broadcast #t))
  (fifo/close fifo broadcast))

(define (fifo/queued fifo (result #f))
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	(set! result (subseq (fifo-queue fifo)
			     (fifo-start fifo)
			     (fifo-end fifo))))
    (condvar-unlock (fifo-state fifo))))
(define (fifo-queued fifo) (fifo/queued fifo))

(define (fifo/load fifo)
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	     (- (fifo-end fifo) (fifo-start fifo)))
    (condvar-unlock (fifo-state fifo))))
(define (fifo-load fifo) (- (fifo-end fifo) (fifo-start fifo)))

(define (fifo/waiting fifo (nonzero #t))
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	(when nonzero
	  (while (zero? (fifo-waiting fifo))
	    (condvar-wait (fifo-state fifo))))
	(fifo-waiting fifo))
    (condvar-unlock (fifo-state fifo))))

(define (fifo/idle? fifo)
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	(until (and (not (zero? (fifo-waiting fifo)))
		    (= (fifo-start fifo) (fifo-end fifo)))
	  (condvar-wait (fifo-state fifo)))
	(and (not (zero? (fifo-waiting fifo)))
	     (= (fifo-start fifo) (fifo-end fifo))))
    (condvar-unlock (fifo-state fifo))))

(define (fifo/pause! fifo value)
  (unwind-protect 
      (begin (condvar-lock (fifo-state fifo))
	(set-fifo-pause! fifo value)
	(condvar-signal (fifo-state fifo) #t))
    (condvar-unlock (fifo-state fifo))))
(define (fifo/paused? fifo) (fifo-pause fifo))

(define (fifo/set-debug! fifo (flag #t)) (set-fifo-debug! fifo flag))
