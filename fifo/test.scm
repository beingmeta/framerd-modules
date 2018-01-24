;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'fifo/test)

(use-module '{fifo logger optimize})

(module-export! '{fifo/test/start fifo/test/run test-fifo})

(define %loglevel %info%)

(config! 'logthreadinfo #t)

(optimize! 'fifo)

(define test-fifo (fifo/make "TESTING"))

(define (random-push fifo (wait 5))
  (let ((v (random 1000000)))
    (fifo/push! fifo v)
    (loginfo |Push| "Pushed " v " onto " fifo))
  (sleep wait))

(define (random-pop fifo (wait 5))
  (let ((v (fifo/pop fifo)))
    (loginfo |Pop| "Popped " v " from " fifo))
  (sleep wait))

(define (pusher fifo duration sleepfor (start (elapsed-time)))
  (while (< (elapsed-time start) duration)
    (random-push fifo (* sleepfor (random 10)))))
(define (popper fifo duration sleepfor (start (elapsed-time)))
  (while (< (elapsed-time start) duration)
    (random-pop fifo (* sleepfor (random 10)))))

(define (parallel-test fifo (duration 10) (sleepfor 1.0))
  (parallel (pusher fifo duration sleepfor)
	    (pusher fifo duration sleepfor)
	    (popper fifo duration sleepfor)
	    (popper fifo duration sleepfor)))

(define (fifo/test/run (n (config 'duration 30)) (sleepfor (config 'sleep 0.1)))
  "This should test pausing"
  (let ((fifo (fifo/make "TESTING")))
    (dotimes (i 25) (random-push fifo 0))
    (let ((wrapper-thread (thread/call parallel-test fifo n sleepfor))
	  (extreme-size (fifo/waiting fifo))))))

(define (fifo/test/start (n (config 'duration 30)) (sleepfor (config 'sleep 0.1)))
  (thread/call parallel-test test-fifo n sleepfor)
  test-fifo)

