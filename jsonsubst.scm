;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'jsonsubst)

(use-module '{texttools fdweb})

(module-export! '{json/subst})

(define var-pattern
  {#("{{" (label var (+ {(isalnum) "-" "_" "/" ":"}) #t) "}}")
   #("{{" (label var (+ {(isalnum) "-" "_" "/" ":"}) #t) "|"
     (label default (not> "}}")) "}}")})

(defambda (subst arg bindings)
  "Descends <arg> substituting {{x} and {{x|default}} expressions"
  (cond ((ambiguous? arg) (for-choices arg (subst arg bindings)))
	((and (string? arg) (textmatch var-pattern arg))
	 (let* ((match (text->frame var-pattern arg))
		(var (get match 'var))
		(default (get match 'default)))
	   (if (testopt bindings var)
	       (getopt bindings var)
	       (if (exists? default) 
		   (handle-default default)
		   (irritant var |NoBinding| jsonsubst)))))
	((pair? arg)
	 (cons (subst (car arg) bindings)
	       (subst (cdr arg) bindings)))
	((vector? arg)
	 (map (lambda (x) (subst x bindings)) arg))
	((table? arg)
	 (let ((keys (getkeys arg))
	       (result (frame-create #f)))
	   (do-choices (key keys)
	     (store! result key (subst (get arg key) bindings)))
	   result))
	(else arg)))

(define (handle-default string (fc #f))
  (when (> (length string) 0) (set! fc (elt string 0)))
  (if (empty-string? string) ""
      (if (eq? fc #\\) (slice string 1)
	  (if (eq? fc #\:)
	      (string->lisp (slice string 1))
	      string))))

(define (json/subst input bindings . args)
  "Does substitution for a JSON structure.\n\
   This descends the structure of <input> (pairs, tables, vectors, etc), 
   substituting {{x} and {{x|default}} expressions along the way. \
   The output structure is converted into a string using ->JSON with
   the calls remaining args (if any).\n\
   If <input> is a string, it is parsed as JSON and used as input."
  (apply ->json
	 (if (string? input)
	     (subst (jsonparse input) bindings)
	     (subst input bindings))
	 args))
