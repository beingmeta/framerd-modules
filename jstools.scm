;;; -*- Mode: Scheme; -*-

(in-module 'jstools)

(use-module '{fdweb texttools})

(module-export! '{javascript/refpat javascript/defpat
		  javascript/files javascript/source
		  javascript/getrefs javascript/getdefs})

(define (javascript/files path)
  (pick (getfiles path) has-suffix ".js"))
(define (javascript/source path)
  (filestring (pick (getfiles path) has-suffix ".js")))

(define (javascript/refpat prefix)
  `#((word ,prefix) (+ #("." (csymbol))) (subst #((spaces*) (ispunct)) "")))

(define (javascript/defpat prefix)
  `#((word ,prefix) (+ #("." (csymbol)))
     (subst #((spaces*) "=") "")))

(define (getrefs prefix sources)
  (let ((canonical (pick-one (largest prefix))))
    (string-subst (gathersubst (javascript/refpat prefix) sources)
		  (qc (difference prefix canonical)) canonical)))
(define javascript/getrefs (ambda (p s) (getrefs (qc p) (qc s))))

(define (getdefs prefix sources)
  (let ((canonical (pick-one (largest prefix))))
    (string-subst (gathersubst (javascript/defpat prefix) sources)
		  (qc (difference prefix canonical)) canonical)))
(define javascript/getdefs (ambda (p s) (getdefs (qc p) (qc s))))


