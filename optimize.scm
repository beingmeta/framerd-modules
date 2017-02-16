;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

;;; Optimizing code structures for the interpreter, including
;;;  use of constant OPCODEs and relative lexical references
(in-module 'optimize)

(define-init standard-modules
  (choice (get (get-module 'reflection) 'getmodules)
	  'scheme 'xscheme 'fileio 'filedb 'history 'logger))
(define-init check-module-usage #f)

;; This module optimizes an expression or procedure by replacing
;; certain variable references with their values directly, which
;; avoids many environment lookups.  The trick is to not replace
;; anything which will change and so produce an equivalent expression
;; or function which just runs faster.

(use-module 'reflection)
(use-module 'varconfig)
(use-module 'logger)

(define-init %loglevel %warning%)
(define-init useopcodes #t)
(define-init optdowarn #t)
(define-init lexrefs-default #t)
(define-init staticfns-default #f)
(define-init fcnrefs-default #t)
(varconfig! optimize:lexrefs lexrefs-default)
(varconfig! optimize:staticfns staticfns-default)
(varconfig! optimize:fcnrefs fcnrefs-default)

(defslambda (codewarning warning)
  (debug%watch "CODEWARNING" warning)
  (threadset! 'codewarnings (choice warning (threadget 'codewarnings))))

(define-init module? 
  (lambda (arg) 
    (and (table? arg) (not (or (environment? arg) (pair? arg))))))

(define opcodeopt-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) useopcodes)
	  (val (set! useopcodes #t))
	  (else (set! useopcodes #f)))))
(config-def! 'opcodeopt opcodeopt-config)

(define optdowarn-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) optdowarn)
	  (val (set! optdowarn #t))
	  (else (set! optdowarn #f)))))
(config-def! 'optdowarn optdowarn-config)

(define-init static-refs (make-hashtable))

(define-init fcnids (make-hashtable))

(module-export! '{optimize! optimized optimized?
		  optimize-procedure! optimize-module!
		  reoptimize! optimize-bindings!
		  deoptimize! deoptimize-procedure! deoptimize-module!
		  deoptimize-bindings!})

(define %volatile '{optdowarn useopcodes %loglevel})

(varconfig! optimize:checkusage check-module-usage)

;;; Utility functions

(define-init special-form-optimizers (make-hashtable))

(define (arglist->vars arglist)
  (if (pair? arglist)
      (cons (if (pair? (car arglist)) (car (car arglist)) (car arglist))
	    (arglist->vars (cdr arglist)))
      (if (null? arglist) '() (list arglist))))

(define (get-lexref sym bindlist base)
  (if (null? bindlist) #f
      (if (pair? (car bindlist))
	  (let ((pos (position sym (car bindlist))))
	    (if pos (%lexref base pos)
		(get-lexref sym (cdr bindlist) (1+ base))))
	  (if (null? (car bindlist))
	      (get-lexref sym (cdr bindlist) (1+ base))
	      (if (eq? (car bindlist) sym) sym
		  (get-lexref sym (cdr bindlist) base))))))

(define (isbound? var bindlist)
  (if (null? bindlist) #f
      (if (pair? (car bindlist))
	  (position var (car bindlist))
	  (if (null? (car bindlist))
	      (isbound? var (cdr bindlist))
	      (or (eq? (car bindlist) var)
		  (isbound? var (cdr bindlist)))))))

;;; Converting non-lexical function references

(define (fcnref value sym env opts)
  (if (and (applicable? value) (cons? value))
      (let ((usevalue
	     (if (getopt opts 'staticfns staticfns-default)
		 (static-ref value)
		 value)))
	(if (and (symbol? sym) 
		 (test env sym value)
		 (getopt opts 'fcnrefs fcnrefs-default))
	    (try (get fcnids (cons sym env))
		 (add-fcnid sym env value))
	    value))
      value))

(defslambda (add-fcnid sym env value)
  (try (get fcnids (cons sym env))
       (let ((newid (fcnid/register value)))
	 (store! fcnids (cons sym env) newid)
	 newid)))

(define (get-fcnid sym env value)
  (if (cons? value)
      (try (get fcnids (cons sym env))
	   (add-fcnid sym env value))
      value))

(define (static-ref ref)
  (if (static? ref) ref
      (try (get static-refs ref)
	   (make-static-ref ref))))

(defslambda (make-static-ref ref)
  (try (get static-refs ref)
       (let ((copy (static-copy ref)))
	 (if (eq? copy ref) ref
	     (begin (store! static-refs ref copy)
	       copy)))))

(defslambda (update-fcnid! var env value)
  (when (test env var value)
    (let ((id (get fcnids (cons var env))))
      (if (exists? id)
	  (fcnid/set! id value)
	  (add-fcnid var env value)))))

;;; Opcode mapping

;;; When opcodes are introduced, the idea is that a given primitive 
;;;  may map into an opcode to be zippily executed in the evaluator.
;;; The opcode-map is a table for identifying the opcode for a given procedure.
;;;  It's keys are either primitive (presumably) procedures or pairs
;;;   of procedures and integers (number of subexpressions).  This allows
;;;   opcode substitution to be limited to simpler forms and allows opcode
;;;   execution to make assumptions about the number of arguments.  Note that
;;;   this means that this file needs to be synchronized with src/scheme/eval.c

(define opcode-map (make-hashtable))

(define (map-opcode value (length #f))
  (if useopcodes
      (try (get opcode-map (cons value length))
	   (get opcode-map value)
	   value)
      value))

(define (def-opcode prim code (n-args #f))
  (if n-args
      (add! opcode-map (cons prim n-args) (make-opcode code))
      (add! opcode-map prim (make-opcode code))))

(when (bound? make-opcode)
  (def-opcode QUOTE      0x00)
  (def-opcode BEGIN      0x01)
  (def-opcode AND        0x02)
  (def-opcode OR         0x03)
  (def-opcode NOT        0x04)
  (def-opcode FAIL       0x05)
  (def-opcode %MODREF    0x06)
  (def-opcode COMMENT    0x07)

  (def-opcode IF         0x10)
  (def-opcode WHEN       0x11)
  (def-opcode UNLESS     0x12)
  (def-opcode IFELSE     0x13)

  (def-opcode AMBIGUOUS? 0x20 1)
  (def-opcode SINGLETON? 0x21 1)
  (def-opcode FAIL?      0x22 1)
  (def-opcode EMPTY?     0x22 1)
  (def-opcode EXISTS?    0x23 1)
  (def-opcode SINGLETON  0x24 1)
  (def-opcode CAR        0x25 1)
  (def-opcode CDR        0x26 1)
  (def-opcode LENGTH     0x27 1)
  (def-opcode QCHOICE    0x28 1)
  (def-opcode CHOICE-SIZE 0x29 1)
  (def-opcode PICKOIDS    0x2A 1)
  (def-opcode PICKSTRINGS 0x2B 1)
  (def-opcode PICK-ONE    0x2C 1)
  (def-opcode IFEXISTS    0x2D 1)

  (def-opcode 1-         0x40 1)
  (def-opcode -1+        0x40 1)
  (def-opcode 1+         0x41 1)
  (def-opcode NUMBER?    0x42 1)
  (def-opcode ZERO?      0x43 1)
  (def-opcode VECTOR?    0x44 1)
  (def-opcode PAIR?      0x45 1)
  (def-opcode NULL?      0x46 1)
  (def-opcode STRING?    0x47 1)
  (def-opcode OID?       0x48 1)
  (def-opcode SYMBOL?    0x49 1)
  (def-opcode FIRST      0x4A 1)
  (def-opcode SECOND     0x4B 1)
  (def-opcode THIRD      0x4C 1)
  (def-opcode ->NUMBER   0x4D 1)

  (def-opcode =          0x60 2)
  (def-opcode >          0x61 2)
  (def-opcode >=         0x62 2)
  (def-opcode <          0x63 2)
  (def-opcode <=         0x64 2)
  (def-opcode +          0x65 2)
  (def-opcode -          0x66 2)
  (def-opcode *          0x67 2)
  (def-opcode /~         0x68 2)

  (def-opcode EQ?        0x80 2)
  (def-opcode EQV?       0x81 2)
  (def-opcode EQUAL?     0x82 2)
  (def-opcode ELT        0x83 2)

  (def-opcode GET        0xA0 2)
  (def-opcode TEST       0xA1 3)
  ;; (def-opcode XREF       0xA2 3)
  (def-opcode %GET       0xA3 2)
  (def-opcode %TEST      0xA4 3)

  (def-opcode IDENTICAL?   0xC0)
  (def-opcode OVERLAPS?    0xC1)
  (def-opcode CONTAINS?    0xC2)
  (def-opcode UNION        0xC3)
  (def-opcode INTERSECTION 0xC4)
  (def-opcode DIFFERENCE   0xC5)
  )

;;; The core loop

(define dont-touch-decls '{%unoptimized %volatile %nosubst})

(defambda (optimize expr env bound opts lexrefs)
  (logdebug "Optimizing " expr " given " bound)
  (cond ((ambiguous? expr)
	 (for-choices (each expr) 
	   (optimize each env bound opts lexrefs)))
	((fail? expr) expr)
	((symbol? expr) (optimize-symbol expr env bound opts lexrefs))
	((not (pair? expr)) expr)
	((or (pair? (car expr)) (ambiguous? (car expr)))
	 ;; We assume that special forms always have a symbol in the
	 ;; head, so anything else we just optimize as a function call.
	 (optimize-call expr env bound opts lexrefs))
	((and (symbol? (car expr)) 
	      (not (symbol-bound? (car expr) env))
	      (not (get-lexref (car expr) bound 0))
	      (not (test env '%nowarn (car expr))))
	 (codewarning (cons* 'UNBOUND expr bound))
	 (when optdowarn
	   (warning "The symbol " (car expr) " in " expr
		    " appears to be unbound given bindings "
		    (apply append bound)))
	 expr)
	(else (optimize-expr expr env bound opts lexrefs))))

(defambda (optimize-symbol expr env bound opts lexrefs)
  (let ((lexref (get-lexref expr bound 0))
	(use-opcodes (getopt opts 'opcodes useopcodes)))
    (debug%watch "OPTIMIZE/SYMBOL" expr lexref env bound)
    (if lexref
	(if lexrefs lexref expr)
	(let* ((srcenv (wherefrom expr env))
	       (module (and srcenv (module? srcenv) srcenv))
	       (value (and srcenv (get srcenv expr))))
	  (debug%watch "OPTIMIZE/SYMBOL/module" 
	    expr module srcenv env bound optdowarn)
	  (when (and module (module? env))
	    (add! env '%free_vars expr)
	    (when (and module (table? module))
	      (add! env '%used_modules
		    (pick (get module '%moduleid) symbol?))))
	  (cond ((and (not srcenv) (test env '%nowarn expr)))
		((not srcenv)
		 (codewarning (cons* 'UNBOUND expr bound))
		 (when env
		   (add! env '%warnings (cons* 'UNBOUND expr bound)))
		 (when optdowarn
		   (warning "The symbol " expr
			    " appears to be unbound given bindings "
			    (apply append bound)))
		 expr)
		((not module) expr)
		((%test srcenv '%nosubst expr) expr)
		((%test srcenv '%constants expr)
		 (let ((v (%get srcenv expr)))
		   (if (or (pair? v) (symbol? v) (ambiguous? v))
		       (list 'quote (qc v))
		       v)))
		((not module) expr)
		(else `(,(try (tryif use-opcodes (map-opcode %modref))
			      %modref)
			,module ,expr)))))))

(define (do-rewrite rewriter expr env bound opts lexrefs)
  (onerror
      (optimize
       (rewriter expr)
       env bound opts lexrefs)
      (lambda (ex)
	(logwarn |RewriteError| 
	  "Error rewriting " expr " with " rewriter)
	(logwarn |RewriteError| "Error rewriting " expr ": " ex)
	expr)))

(define (check-arguments value n-exprs expr)
  (when (and (procedure-min-arity value)
	     (< n-exprs (procedure-min-arity value)))
    (codewarning (list 'TOOFEWARGS expr value))
    (when optdowarn
      (warning "The call to " expr
	       " provides too few arguments "
	       "(" n-exprs ") for " value)))
  (when (and (procedure-arity value)
	     (> n-exprs (procedure-arity value)))
    (codewarning (list 'TOOMANYARGS expr value))
    (when optdowarn
      (warning "The call to " expr " provides too many "
	       "arguments (" n-exprs ") for " value))))

(define (optimize-expr expr env bound opts lexrefs)
  (let* ((head (get-arg expr 0))
	 (use-opcodes (getopt opts 'opcodes useopcodes))
	 (n-exprs (-1+ (length expr)))
	 (value (if (symbol? head)
		    (or (get-lexref head bound 0) (get env head))
		    head))
	 (from (and (symbol? head)
		    (not (get-lexref head bound 0))
		    (wherefrom head env))))
    (when (and from (module? env))
      (add! env '%free_vars expr)
      (when (and from (table? from))
	(add! env '%used_modules
	      (pick (get from '%moduleid) symbol?))))
    (cond ((or (and from (test from dont-touch-decls head))
	       (and env (test env dont-touch-decls head)))
	   expr)
	  ((and from (%test from '%rewrite)
		(%test (get from '%rewrite) head))
	   (do-rewrite (get (get from '%rewrite) head) 
		       expr env bound opts lexrefs))
	  ((and (ambiguous? value)
		(or (exists special-form? value)
		    (exists macro? value)))
	   (logwarn |CantOptimize|
	     "Ambiguous head includes a macro or special form"
	     expr)
	   expr)
	  ((fail? value)
	   (logwarn |CantOptimize|
	     "The head's value is the empty choice"
	     expr)
	   expr)
	  ((exists special-form? value)
	   (let* ((optimizer
		   (try (get special-form-optimizers value)
			(get special-form-optimizers
			     (procedure-name value))
			(get special-form-optimizers head)))
		  (transformed (try (optimizer value expr env bound opts lexrefs)
				    expr))
		  (newhead (car transformed)))
	     (cons (try (get opcode-map newhead)
			(tryif (getopt opts 'fcnrefs fcnrefs-default)
			  (get-fcnid head #f newhead))
			newhead)
		   (cdr transformed))))
	  ((exists macro? value)
	   (optimize (macroexpand value expr) env
		     bound opts lexrefs))
	  ((fail? (reject value applicable?))
	   (check-arguments value n-exprs expr)
	   (callcons (qc (fcnref
			  (map-opcode
			   (cond ((not from) (try value head))
				 ((fail? value) 
				  `(,(try (tryif use-opcodes (map-opcode %modref))
					  %modref)
				    ,from ,head))
				 ((test from '%nosubst head) head)
				 ((test from '%volatile head)
				  (try (tryif use-opcodes (map-opcode %modref))
				       %modref))
				 (else value))
			   n-exprs)
			  head from opts))
		     (optimize-args (cdr expr) env bound opts lexrefs)))
	  (else
	   (when (and optdowarn from
		      (not (test from '{%nosubst %volatile} head)))
	     (codewarning (cons* 'NOTFCN expr value))
	     (warning "The current value of " expr " (" head ") "
		      "doesn't appear to be a applicable given "
		      (apply append bound)))
	   expr))))

(define (callcons head tail)
  (cons head (->list tail)))

(defambda (optimize-call expr env bound opts lexrefs)
  (if (pair? expr)
      (if (or (symbol? (car expr)) (pair? (car expr))
	      (ambiguous? (car expr)))
	  `(,(optimize (car expr) env bound opts lexrefs)
	    ,@(if (pair? (cdr expr))
		  (optimize-args (cdr expr) env bound opts lexrefs)
		  (cdr expr)))
	  (optimize-args expr env bound opts lexrefs))
      expr))
(define (optimize-head expr env bound opts lexrefs)
  ())
(defambda (optimize-args expr env bound opts lexrefs)
  (forseq (arg expr)
    (if (or (qchoice? arg) (fail? arg)) 
	arg
	(optimize arg env bound opts lexrefs))))

(define (optimize-procedure! proc (opts #f) (lexrefs))
  (default! lexrefs (getopt opts 'lexrefs lexrefs-default))
  (threadset! 'codewarnings #{})
  (let* ((env (procedure-env proc))
	 (arglist (procedure-args proc))
	 (body (procedure-body proc))
	 (bound (list (arglist->vars arglist)))
	 (initial (if (pair? body) (car body)
		      (and (rail? body) (> (length body) 0)
			   (elt body 0))))
	 (new-body `((comment |original| ,@(->list body))
		     (comment |originalargs| 
			      ,(if (rail? arglist) (->list arglist) arglist))
		     ,@(map (lambda (b)
			      (optimize b env bound opts lexrefs))
			    (->list body)))))
    (unless (and initial 
		 (or (and (pair? initial)
			  (eq? (car initial) 'COMMENT)
			  (pair? (cdr initial))
			  (eq? (cadr initial) '|original|))
		     (and (rail? initial) (> (length initial) 1)
			  (eq? (elt initial 0) 'COMMENT)
			  (eq? (elt initial 1) '|original|))))
      (set-procedure-body! proc new-body)
      (when (pair? arglist)
	(let ((optimized-args 
	       (optimize-arglist arglist env opts lexrefs)))
	  (unless (equal? arglist optimized-args)
	    (set-procedure-args! proc optimized-args))))
      (when (exists? (threadget 'codewarnings))
	(warning "Errors optimizing " proc ": "
		 (do-choices (warning (threadget 'codewarnings))
		   (printout "\n\t" warning)))
	(threadset! 'codewarnings #{})))))

(define (optimize-arglist arglist env opts lexrefs)
  (if (pair? arglist)
      (cons
       (if (and (pair? (car arglist)) (pair? (cdr (car arglist)))
		(singleton? (cadr (car arglist))))
	   `(,(caar arglist) 
	     ,(optimize (cadr (car arglist)) env '() opts lexrefs))
	   (car arglist))
       (optimize-arglist (cdr arglist) env opts lexrefs))
      arglist))
  
(define (deoptimize-procedure! proc)
  (let* ((body (procedure-body proc))
	 (exprs (->list (pick (pick (elts body) {rail? pair?}) length >= 3)))
	 (original-body (pick exprs car 'comment cadr '|original|))
	 (original-args (pick exprs car 'comment cadr '|originalargs|)))
    (cond ((and (singleton? original-body)  (singleton? original-args))
	   (set-procedure-body! proc original-body)
	   (set-procedure-body! proc original-args)
	   #t)
	  (else #f))))

(define (optimized? arg)
  (if (compound-procedure? arg)
      (let* ((body (procedure-body arg))
	     (exprs (->list (pick (pick (elts body) {rail? pair?}) length >= 3)))
	     (original-body (pick exprs car 'comment cadr '|original|))
	     (original-args (pick exprs car 'comment cadr '|originalargs|)))
	(and (exists? original-body) (exists? original-args)))
      (and (or (hashtable? arg) (slotmap? arg) (schemap? arg)
	       (environment? arg))
	   (some? (lambda (var)
		    (and (test arg var) 
			 (exists compound-procedure? (get arg var))
			 (exists optimized?
				 (pick (get arg var) compound-procedure?))))
		  (choice->vector (getkeys arg))))))

(define (optimize-get-module spec)
  (onerror (get-module spec)
    (lambda (ex) 
      (irritant+ spec |GetModuleFailed| optimize-module
		 "Couldn't load module " spec))))

(define (optimize-module! module (opts) (lexrefs))
  (loginfo "Optimizing module " module)
  (when (symbol? module)
    (set! module (optimize-get-module module)))
  (default! opts (try (get module '%optimize_options) #f))
  (default! lexrefs (getopt opts 'lexrefs lexrefs-default))
  (let ((bindings (module-bindings module))
	(usefcnrefs (getopt opts 'fcnrefs fcnrefs-default))
	(count 0))
    (do-choices (var bindings)
      (loginfo "Optimizing module binding " var)
      (let ((value (get module var)))
	(when (and (exists? value) (compound-procedure? value))
	  (set! count (1+ count))
	  (optimize-procedure! value opts lexrefs))
	(when (and usefcnrefs (exists? value) (applicable? value))
	  (update-fcnid! var module value))))
    (when (exists symbol? (get module '%moduleid))
      (let* ((referenced-modules (get module '%used_modules))
	     (used-modules
	      (eval `(within-module ',(pick (get module '%moduleid) symbol?)
				    (,getmodules))))
	     (unused (difference used-modules referenced-modules standard-modules
				 (get module '%moduleid))))
	(when (and check-module-usage (exists? unused))
	  (logwarn |UnusedModules|
	    "Module " (try (pick (get module '%moduleid) symbol?)
			   (get module '%moduleid))
	    " declares " (choice-size unused) " possibly unused modules: "
	    (do-choices (um unused i)
	      (printout (if (> i 0) ", ") um))))))
    count))

(define (deoptimize-module! module)
  (loginfo "Deoptimizing module " module)
  (when (symbol? module)
    (set! module (optimize-get-module module)))
  (let ((bindings (module-bindings module))
	(count 0))
    (do-choices (var bindings)
      (loginfo "Deoptimizing module binding " var)
      (let ((value (get module var)))
	(when (and (exists? value) (compound-procedure? value))
	  (when (deoptimize-procedure! value)
	    (set! count (1+ count))))))
    count))

(define (optimize-bindings! bindings (opts #f) (lexrefs))
  (default! lexrefs (getopt opts 'lexrefs lexrefs-default))
  (logdebug "Optimizing bindings " bindings)
  (let ((count 0)
	(skip (getopt opts 'dont-optimize
		      (tryif (test bindings '%dont-optimize)
			(get bindings '%dont-optimize)))))
    (do-choices (var (difference (getkeys bindings) skip '%dont-optimize))
      (logdebug "Optimizing binding " var)
      (let ((value (get bindings var)))
	(if (bound? value)
	    (when (compound-procedure? value)
	      (set! count (1+ count))
	      (optimize-procedure! value #f lexrefs))
	    (warning var " is unbound"))))
    count))

(define (deoptimize-bindings! bindings (opts #f) (lexrefs))
  (logdebug "Deoptimizing bindings " bindings)
  (let ((count 0))
    (do-choices (var (getkeys bindings))
      (logdetail "Deoptimizing binding " var)
      (let ((value (get bindings var)))
	(if (bound? value)
	    (when (compound-procedure? value)
	      (when (deoptimize-procedure! value)
		(set! count (1+ count))))
	    (warning var " is unbound"))))
    count))

(defambda (module-arg? arg)
  (or (fail? arg) (string? arg)
      (and (pair? arg) (eq? (car arg) 'quote))
      (and (ambiguous? arg) (fail? (reject arg module-arg?)))))

(define (optimize*! . args)
  (dolist (arg args)
    (cond ((compound-procedure? arg) (optimize-procedure! arg))
	  ((table? arg) (optimize-module! arg))
	  (else (error '|TypeError| 'optimize*
			 "Invalid optimize argument: " arg)))))

(define (deoptimize*! . args)
  (dolist (arg args)
    (cond ((compound-procedure? arg) (deoptimize-procedure! arg))
	  ((table? arg) (deoptimize-module! arg))
	  (else (error '|TypeError| 'optimize*
			 "Invalid optimize argument: " arg)))))

(define optimize!
  (macro expr
    (if (null? (cdr expr))
	`(,optimize-bindings! (,%bindings))
	(cons optimize*!
	      (map (lambda (x)
		     (if (module-arg? x)
			 `(,optimize-get-module ,x)
			 x))
		   (cdr expr))))))

(define deoptimize!
  (macro expr
    (if (null? (cdr expr))
	`(,deoptimize-bindings! (,%bindings))
	(cons deoptimize*!
	      (map (lambda (x)
		     (if (module-arg? x)
			 `(,optimize-get-module ,x)
			 x))
		   (cdr expr))))))

(defambda (reoptimize! modules)
  (reload-module modules)
  (optimize-module! (get-module modules)))

(define (optimized arg)
  (cond ((compound-procedure? arg) (optimize-procedure! arg))
	((or (hashtable? arg) (slotmap? arg) (schemap? arg)
	     (environment? arg))
	 (optimize-module! arg))
	(else (irritant arg |TypeError| OPTIMIZED
			"Not a compound procedure, environment, or module")))
  arg)

;;;; Special form handlers

(define (optimize-block handler expr env bound opts lexrefs)
  (cons (map-opcode handler (length (cdr expr)))
	(map (lambda (x) (optimize x env bound opts lexrefs))
	     (cdr expr))))

(define (optimize-let handler expr env bound opts lexrefs)
  (let* ((bindexprs (cadr expr))
	 (new-bindexprs
	  (map (lambda (x)
		 `(,(car x) ,(optimize (cadr x) env bound opts lexrefs)))
	       bindexprs))
	 (body (cddr expr)))
    `(,handler ,new-bindexprs
	       ,@(let ((bound (cons (map car bindexprs) bound)))
		   (map (lambda (b) (optimize b env bound opts lexrefs))
			body)))))
(define (optimize-doexpression handler expr env bound opts lexrefs)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec)
		,(optimize (cadr bindspec) env bound opts lexrefs)
		,@(cddr bindspec))
	       ,@(let ((bound
			(if (= (length bindspec) 3)
			    (cons (list (first bindspec) (third bindspec))
				  bound)
			    (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (optimize b env bound opts lexrefs))
			body)))))
(define (optimize-do2expression handler expr env bound opts lexrefs)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler ,(cond ((pair? bindspec)
		       `(,(car bindspec)
			 ,(optimize (cadr bindspec) env bound opts lexrefs)
			 ,@(cddr bindspec)))
		      ((symbol? bindspec)
		       `(,bindspec
			 ,(optimize bindspec env bound opts lexrefs)))
		      (else (error 'syntax "Bad do-* expression")))
	       ,@(let ((bound
			(if (symbol? bindspec)
			    (cons (list bindspec) bound)
			    (if (= (length bindspec) 3)
				(cons (list (first bindspec)
					    (third bindspec)) bound)
				(cons (list (first bindspec)) bound)))))
		   (map (lambda (b) (optimize b env bound opts lexrefs))
			body)))))

(define (optimize-dosubsets handler expr env bound opts lexrefs)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler (,(car bindspec)
		,(optimize (cadr bindspec) env bound opts lexrefs)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 4)
				  (cons (list (first bindspec) (fourth bindspec))
					bound)
				  (cons (list (first bindspec)) bound))))
		   (map (lambda (b) (optimize b env bound opts lexrefs))
			body)))))

(define (optimize-let*-bindings bindings env bound opts lexrefs)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(optimize (cadr (car bindings)) env bound opts lexrefs))
	,@(optimize-let*-bindings
	   (cdr bindings) env
	   (cons (append (car bound) (list (car (car bindings))))
		 (cdr bound))
	   opts lexrefs))))

(define (optimize-let* handler expr env bound opts lexrefs)
  (let ((bindspec (cadr expr)) (body (cddr expr)))
    `(,handler
      ,(optimize-let*-bindings (cadr expr) env (cons '() bound) opts lexrefs)
      ,@(let ((bound (cons (map car bindspec) bound)))
	  (map (lambda (b) (optimize b env bound opts lexrefs))
	       body)))))
(define (optimize-set-form handler expr env bound opts lexrefs)
  `(,handler ,(cadr expr) ,(optimize (third expr) env bound opts lexrefs)))

(define (optimize-lambda handler expr env bound opts lexrefs)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (cons (arglist->vars (cadr expr)) bound)))
		 (map (lambda (b) (optimize b env bound opts lexrefs))
		      (cddr expr)))))

(define (optimize-cond handler expr env bound opts lexrefs)
  (cons handler 
	(map (lambda (clause)
	       (cond ((eq? (car clause) 'else)
		      `(ELSE
			,@(map (lambda (x)
				 (optimize x env bound opts lexrefs))
			       (cdr clause))))
		     ((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
		      `(,(optimize (car clause) env bound opts lexrefs)
			=>
			,@(map (lambda (x)
				 (optimize x env bound opts lexrefs))
			       (cddr clause))))
		     (else (map (lambda (x)
				  (optimize x env bound opts lexrefs))
				clause))))
	     (cdr expr))))

(define (optimize-case handler expr env bound opts lexrefs)
  `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
	     ,@(map (lambda (clause)
		      (cons (car clause)
			    (map (lambda (x)
				   (optimize x env bound opts lexrefs))
				 (cdr clause))))
		    (cddr expr))))

(define (optimize-unwind-protect handler expr env bound opts lexrefs)
  `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
	     ,@(map (lambda (uwclause)
		      (optimize uwclause env bound opts lexrefs))
		    (cddr expr))))

(define (optimize-quasiquote handler expr env bound opts lexrefs)
  `(,handler ,(optimize-quasiquote-node (cadr expr) env bound opts lexrefs)))
(defambda (optimize-quasiquote-node expr env bound opts lexrefs)
  (cond ((ambiguous? expr)
	 (for-choices (elt expr)
	   (optimize-quasiquote-node elt env bound opts lexrefs)))
	((and (pair? expr) 
	      (or (eq? (car expr) 'unquote)  (eq? (car expr) 'unquote*)))
	 `(,(car expr) ,(optimize (cadr expr) env bound opts lexrefs)))
	((pair? expr)
	 (forseq (elt expr)
	   (optimize-quasiquote-node elt env bound opts lexrefs)))
	((vector? expr)
	 (forseq (elt expr)
	   (optimize-quasiquote-node elt env bound opts lexrefs)))
	((slotmap? expr)
	 (let ((copy (frame-create #f))
	       (slots (getkeys expr)))
	   (do-choices (slot slots)
	     (store! copy (optimize-quasiquote-node slot env bound opts lexrefs)
		     (try (optimize-quasiquote-node (get expr slot) env bound opts lexrefs)
			  (get expr slot))))
	   copy))
	(else expr)))

(define (optimize-logmsg handler expr env bound opts lexrefs)
  (if (or (symbol? (cadr expr)) (number? (cadr expr)))
      (if (or (symbol? (caddr expr)) (number? (caddr expr)))
	  `(,handler ,(cadr expr)
		     ,(caddr expr)
		     ,@(map (lambda (elt)
			      (optimize elt env bound opts lexrefs))
			    (cdddr expr)))
	  `(,handler ,(cadr expr)
		     ,@(map (lambda (elt)
			      (optimize elt env bound opts lexrefs))
			    (cddr expr))))
      `(,handler ,@(map (lambda (elt)
			  (optimize elt env bound opts lexrefs))
			(cdr expr)))))

(define (optimize-logif handler expr env bound opts lexrefs)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
		 ,(caddr expr)
		 ,@(map (lambda (elt)
			  (optimize elt env bound opts lexrefs))
			(cdddr expr)))
      `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
		 ,@(map (lambda (elt)
			  (optimize elt env bound opts lexrefs))
			(cddr expr)))))

(define (optimize-logif+ handler expr env bound opts lexrefs)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      (if (or (symbol? (cadr (cddr expr))) (number? (cadr (cddr expr))))
	  `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
		     ,(caddr expr)
		     ,(cadr (cddr expr))
		     ,@(map (lambda (elt)
			      (optimize elt env bound opts lexrefs))
			    (cdr (cdddr expr))))
	  `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
		     ,(caddr expr)
		     ,@(map (lambda (elt)
			      (optimize elt env bound opts lexrefs))
			    (cdddr expr))))
      `(,handler ,(optimize (cadr expr) env bound opts lexrefs)
		 ,@(map (lambda (elt)
			  (optimize elt env bound opts lexrefs))
			(cddr expr)))))

;;; Optimizing XHTML expressions

;; This doesn't handle mixed alist ((x y)) and plist (x y) attribute lists
;;  because they shouldn't work anyway
(define (optimize-attribs attribs env bound opts lexrefs)
  (cond ((not (pair? attribs)) attribs)
	((pair? (cdr attribs))
	 `(,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		`(,(car (car attribs))
		  ,(optimize (cadr (car attribs)) env bound opts lexrefs))
		(car attribs))
	   ,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		(if (pair? (cadr attribs))
		    `(,(car (cadr attribs))
		      ,(optimize (cadr (cadr attribs)) env bound opts lexrefs))
		    (optimize (cadr attribs) env bound opts lexrefs))
		(optimize (cadr attribs) env bound opts lexrefs))
	   ,@(optimize-attribs (cddr attribs) env bound opts lexrefs)))
	((pair? (car attribs))
	 `((,(car (car attribs))
	    ,(optimize (cadr (car attribs)) env bound opts lexrefs))))
	(else attribs)))

(define (optimize-markup handler expr env bound opts lexrefs)
  `(,(car expr)
    ,@(map (lambda (x) (optimize x env bound opts lexrefs))
	   (cdr expr))))
(define (optimize-markup* handler expr env bound opts lexrefs)
  `(,(car expr) ,(optimize-attribs (second expr) env bound opts lexrefs)
    ,@(map (lambda (x) (optimize x env bound opts lexrefs))
	   (cddr expr))))

(define (optimize-emptymarkup fcn expr env bound opts lexrefs)
  `(,(car expr) ,@(optimize-attribs (cdr expr) env bound opts lexrefs)))

(define (optimize-anchor* fcn expr env bound opts lexrefs)
  `(,(car expr) ,(optimize (cadr expr) env bound opts lexrefs)
    ,(optimize-attribs (third expr) env bound opts lexrefs)
    ,@(map (lambda (elt) (optimize elt env bound opts lexrefs))
	   (cdr (cddr expr)))))

(define (optimize-xmlblock fcn expr env bound opts lexrefs)
  `(,(car expr) ,(cadr expr)
    ,(optimize-attribs (third expr) env bound opts lexrefs)
    ,@(map (lambda (elt) (optimize elt env bound opts lexrefs))
	   (cdr (cddr expr)))))

;;; Declare them

(add! special-form-optimizers let optimize-let)
(add! special-form-optimizers let* optimize-let*)
(when (bound? letq)
  (add! special-form-optimizers letq optimize-let)
  (add! special-form-optimizers letq* optimize-let*))
(add! special-form-optimizers (choice lambda ambda slambda)
      optimize-lambda)
(add! special-form-optimizers (choice set! set+! default! define)
      optimize-set-form)

(add! special-form-optimizers
      (choice dolist dotimes doseq forseq)
      optimize-doexpression)
(add! special-form-optimizers
      (choice do-choices for-choices filter-choices try-choices)
      optimize-do2expression)

(add! special-form-optimizers
      (choice begin prog1 try tryif when if unless and or until while)
      optimize-block)
(when (bound? ipeval)
  (add! special-form-optimizers
	(choice ipeval tipeval)
	optimize-block))
(add! special-form-optimizers case optimize-case)
(add! special-form-optimizers cond optimize-cond)
(add! special-form-optimizers do-subsets optimize-dosubsets)
(when (bound? parallel)
  (add! special-form-optimizers
	(choice parallel spawn)
	optimize-block))

(add! special-form-optimizers
      (choice printout lineout stringout message notify)
      optimize-block)

(add! special-form-optimizers unwind-protect optimize-unwind-protect)

(add! special-form-optimizers
      {"ONERROR" "UNWIND-PROTECT" "DYNAMIC-WIND"}
      optimize-block)
(add! special-form-optimizers {"FILEOUT" "SYSTEM"} optimize-block)

(add! special-form-optimizers 
      ({procedure-name (lambda (x) x) 
	(lambda (x) (string->symbol (procedure-name x)))}
       quasiquote)
      optimize-quasiquote)

(add! special-form-optimizers logmsg optimize-logmsg)
(add! special-form-optimizers logif optimize-logif)
(add! special-form-optimizers logif+ optimize-logif+)

;; Don't optimize these because they look at the symbol that is the head
;; of the expression to get their tag name.
(add! special-form-optimizers {"markupblock" "ANCHOR"} optimize-markup)
(add! special-form-optimizers {"markup*block" "markup*"} optimize-markup*)
(add! special-form-optimizers "emptymarkup" optimize-emptymarkup)
(add! special-form-optimizers "ANCHOR*" optimize-anchor*)
(add! special-form-optimizers "XMLBLOCK" optimize-xmlblock)
(add! special-form-optimizers "WITH/REQUEST" optimize-block)
(add! special-form-optimizers "WITH/REQUEST/OUT" optimize-block)
(add! special-form-optimizers "XMLOUT" optimize-block)
(add! special-form-optimizers "XHTML" optimize-block)
(add! special-form-optimizers "XMLEVAL" optimize-block)
(add! special-form-optimizers "GETOPT" optimize-block)
(add! special-form-optimizers "TESTOPT" optimize-block)

(when (bound? fileout)
  (add! special-form-optimizers
	(choice fileout system)
	optimize-block))
