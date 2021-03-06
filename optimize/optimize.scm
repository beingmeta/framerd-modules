;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

;;; DON'T EDIT THIS FILE !!!
;;;
;;; The reference version of this module now in the src/libscm
;;; directory of the FramerD/KNO source tree. Please edit that file
;;; instead.

(in-module 'optimize)
;;; Optimizing code structures for the interpreter, including
;;;  use of constant OPCODEs and relative lexical references

(define-init standard-modules
  (choice (get (get-module 'reflection) 'getmodules)
	  'scheme 'xscheme 'fileio 'history 'logger))
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

;; OPTLEVEL interpretations
;; 0: no optimization
;; 1: fcnrefs + opcodes
;; 2: substs + lexrefs
;; 3: rewrites
(define-init optlevel 3)
(varconfig! optimize:level optlevel)
(varconfig! optlevel optlevel)

(define-init fcnrefs-default {})
(define-init opcodes-default {})
(define-init bindops-default {})
(define-init lexrefs-default {})
(define-init substs-default {})
(define-init rewrite-default {})

(varconfig! optimize:fcnrefs fcnrefs-default)
(varconfig! optimize:opcodes opcodes-default)
(varconfig! optimize:bindops bindpos-default)
(varconfig! optimize:substs  substs-default)
(varconfig! optimize:lexrefs lexrefs-default)
(varconfig! optimize:rewrite rewrite-default)

(define-init staticfns-default #f)
(varconfig! optimize:staticfns staticfns-default)

(define-init persist-default #f)
(varconfig! optimize:persist persist-default)

(define (optmode-macro optname thresh varname)
  (macro expr
    `(getopt ,(cadr expr) ',optname
	     (try ,varname (>= (getopt ,(cadr expr) 'optlevel optlevel)
			       ,thresh)))))
(define optmode
  (macro expr
    (let ((optname (get-arg expr 1))
	  (opthresh (get-arg expr 2))
	  (optvar (get-arg expr 3)))
      `(optmode-macro ',optname ,opthresh ',optvar))))

(define use-opcodes? (optmode opcodes 2 opcodes-default))
(define use-bindops? (optmode bindops 4 bindops-default))
(define use-substs? (optmode substs 2 substs-default))
(define use-lexrefs? (optmode lexrefs 2 lexrefs-default))
(define rewrite? (optmode 'rewrite 3 rewrite-default))

;;; Controls whether optimization warnings are emitted in real time
;;; (when encountered)
(define-init optwarn #t)
(define optwarn-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) optwarn)
	  (val (set! optwarn #t))
	  (else (set! optwarn #f)))))
(config-def! 'optwarn optwarn-config)
(defslambda (codewarning warning)
  (debug%watch "CODEWARNING" warning)
  (threadset! 'codewarnings (choice warning (threadget 'codewarnings))))

;;; What we export

(module-export! '{optimize! optimized optimized?
		  optimize-procedure! optimize-module!
		  reoptimize! optimize-bindings!
		  deoptimize! 
		  deoptimize-procedure!
		  deoptimize-module!
		  deoptimize-bindings!})

;;; Tables

(define-init static-refs (make-hashtable))

(define-init fcnids (make-hashtable))

(define %volatile '{optwarn useopcodes %loglevel})

(varconfig! optimize:checkusage check-module-usage)

;;; Utility functions

(define-init special-form-optimizers (make-hashtable))
(define-init procedure-optimizers (make-hashtable))

(define (arglist->vars arglist)
  (if (pair? arglist)
      (cons (if (pair? (car arglist))
		(car (car arglist))
		(car arglist))
	    (arglist->vars (cdr arglist)))
      (if (null? arglist)
	  '()
	  (list arglist))))

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
  (if (and (cons? value)
	   (or (applicable? value) (special-form? value)))
      (if (special-form? value) 
	  (if (getopt opts 'staticfns staticfns-default)
	      (static-ref value)
	      value)
	  ;; Otherwise:
	  (let ((usevalue
		 (if (getopt opts 'staticfns staticfns-default)
		     (static-ref value)
		     value)))
	    (if (and (symbol? sym) 
		     (test env sym value)
		     (getopt opts 'fcnrefs fcnrefs-default))
		(try (get fcnids (cons sym env))
		     (add-fcnid sym env value))
		value)))
      value))

(defslambda (add-fcnid sym env value)
  (try (if env
	   (get fcnids (cons sym env))
	   (get fcnids env))
       (let ((newid (fcnid/register value)))
	 (store! fcnids 
		 (if env (cons sym env) sym)
		 newid)
	 newid)))

(define (get-fcnid sym env value)
  (if (cons? value)
      (try (get fcnids (cons sym env))
	   (add-fcnid sym env value))
      value))

(define (force-fcnid value)
  (if (cons? value)
      (try (get fcnids value)
	   (add-fcnid value #f value))
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

(define-init opcode-map (make-hashtable))

(define (map-opcode value (opts #f) (length #f))
  (if (use-opcodes? opts)
      (try (tryif length (get opcode-map (cons value length)))
	   (tryif (and length (procedure? value)
		       (procedure-name value)) 
	     (get opcode-map (cons (procedure-name value) length)))
	   (get opcode-map value)
	   (get opcode-map value)
	   (tryif (and (procedure? value) (procedure-name value)) 
	     (get opcode-map (procedure-name value)))
	   value)
      value))

(define name2op
  (and (bound? name->opcode) name->opcode))

(define (def-opcode prim code (n-args #f))
  (if n-args
      (add! opcode-map (cons prim n-args) code)
      (add! opcode-map prim code)))

(define branch-opcode #OP_BRANCH)
(define not-opcode    #OP_NOT)
(define until-opcode  #OP_UNTIL)
(define begin-opcode  #OP_BEGIN)
(define quote-opcode  #OP_QUOTE)
(define assign-opcode #OP_ASSIGN)
(define symref-opcode #OP_SYMREF)
(define bind-opcode   #OP_BIND)
(define void-opcode   #OP_VOID)
(define and-opcode    #OP_AND)
(define or-opcode     #OP_OR)
(define try-opcode    #OP_TRY)
(define xref-opcode   #OP_XREF)
(define choiceref-opcode #OP_CHOICEREF)
(define fixchoice-opcode  #OP_FIXCHOICE)

(def-opcode AMBIGUOUS? #OP_AMBIGP 1)
(def-opcode SINGLETON? #OP_SINGLETONP 1)
(def-opcode EMPTY?     #OP_FAILP 1)
(def-opcode FAIL?      #OP_FAILP 1)
(def-opcode EXISTS?    #OP_EXISTSP 1)
(def-opcode SINGLETON  #OP_SINGLETON 1)
(def-opcode CAR        #OP_CAR 1)
(def-opcode CDR        #OP_CDR 1)
(def-opcode LENGTH     #OP_LENGTH 1)
(def-opcode QCHOICE    #OP_QCHOICE 1)
(def-opcode CHOICE-SIZE #OP_CHOICESIZE 1)
(def-opcode PICKOIDS    #OP_PICKOIDS 1)
(def-opcode PICKSTRINGS #OP_PICKSTRINGS 1)
(def-opcode PICK-ONE    #OP_PICKONE 1)
(def-opcode IFEXISTS    #OP_IFEXISTS 1)

(def-opcode 1-         #OP_MINUS1 1)
(def-opcode -1+        #OP_MINUS1 1)
(def-opcode 1+         #OP_PLUS1 1)
(def-opcode NUMBER?    #OP_NUMBERP 1)
(def-opcode ZERO?      #OP_ZEROP 1)
(def-opcode VECTOR?    #OP_VECTORP 1)
(def-opcode PAIR?      #OP_PAIRP 1)
(def-opcode NULL?      #OP_NILP 1)
(def-opcode STRING?    #OP_STRINGP 1)
(def-opcode OID?       #OP_OIDP 1)
(def-opcode SYMBOL?    #OP_SYMBOLP 1)
(def-opcode FIRST      #OP_FIRST 1)
(def-opcode SECOND     #OP_SECOND 1)
(def-opcode THIRD      #OP_THIRD 1)
(def-opcode CADR       #OP_CADR 1)
(def-opcode CDDR       #OP_CDDR 1)
(def-opcode CADDR      #OP_CADDR 1)
(def-opcode CDDDR      #OP_CDDDR 1)
(def-opcode ->NUMBER   #OP_2NUMBER 1)

(def-opcode =          #OP_NUMEQ 2)
(def-opcode >          #OP_GT 2)
(def-opcode >=         #OP_GTE 2)
(def-opcode <          #OP_LT 2)
(def-opcode <=         #OP_LTE 2)
(def-opcode +          #OP_PLUS 2)
(def-opcode -          #OP_MINUS 2)
(def-opcode *          #OP_MULT 2)
(def-opcode /~         #OP_FLODIV 2)

(def-opcode IDENTICAL?   #OP_IDENTICALP 2)
(def-opcode OVERLAPS?    #OP_OVERLAPSP 2)
(def-opcode CONTAINS?    #OP_CONTAINSP 2)
(def-opcode UNION        #OP_UNION)
(def-opcode INTERSECTION #OP_INTERSECTION)
(def-opcode DIFFERENCE   #OP_DIFFERENCE)
(def-opcode EQ?          #OP_EQP 2)
(def-opcode EQV?         #OP_EQVP 2)
(def-opcode EQUAL?       #OP_EQUALP 2)
(def-opcode ELT          #OP_SEQELT 2)

(def-opcode GET        #OP_FGET 2)
(def-opcode TEST       #OP_FTEST 3)
(def-opcode ASSERT!    #OP_ASSERT 3)
(def-opcode RETRACT!   #OP_RETRACT 3)
(def-opcode %GET       #OP_PGET 2)
(def-opcode %TEST      #OP_PTEST 3)



;;; The core loop

(define dont-touch-decls '{%unoptimized %volatile %nosubst})

(defambda (optimize expr env bound opts)
  (logdebug "Optimizing " expr " given " bound)
  (cond ((ambiguous? expr)
	 (for-choices (each expr) 
	   (optimize each env bound opts)))
	((fail? expr) expr)
	((symbol? expr) (optimize-symbol expr env bound opts))
	((not (pair? expr)) expr)
	((or (pair? (car expr)) (ambiguous? (car expr)))
	 ;; We assume that special forms always have a symbol in the
	 ;; head, so anything else we just optimize as a function call.
	 (optimize-call expr env bound opts))
	((and (symbol? (car expr)) 
	      (not (symbol-bound? (car expr) env))
	      (not (get-lexref (car expr) bound 0))
	      (not (test env '%nowarn (car expr))))
	 (codewarning (cons* 'UNBOUND expr bound))
	 (when optwarn
	   (warning "The symbol " (car expr) " in " expr
		    " appears to be unbound given bindings "
		    (apply append bound)))
	 expr)
	(else (optimize-expr expr env bound opts))))

(define optimize-body
  (macro expr
    (let ((body-expr (get-arg expr 1))
	  (env-expr (get-arg expr 2 'env))
	  (bound-expr (get-arg expr 3 'bound))
	  (opts-expr (get-arg expr 4 'opts)))
      `(forseq (_subexpr ,body-expr) 
	 (optimize _subexpr ,env-expr ,bound-expr ,opts-expr)))))

(defambda (optimize-symbol expr env bound opts)
  (let ((lexref (get-lexref expr bound 0))
	(use-opcodes (use-opcodes? opts)))
    (debug%watch "OPTIMIZE/SYMBOL" expr lexref env bound)
    (if lexref
	(if (use-lexrefs? opts) lexref expr)
	(let* ((srcenv (wherefrom expr env))
	       (module (and srcenv (module? srcenv) srcenv))
	       (value (and srcenv (get srcenv expr))))
	  (debug%watch "OPTIMIZE/SYMBOL/module" 
	    expr module srcenv env bound optwarn)
	  (when (and module (module? env))
	    (add! env '%symrefs expr)
	    (when (and module (table? module))
	      (add! env '%modrefs
		    (pick (get module '%moduleid) symbol?))))
	  (cond ((and (not srcenv)
		      (or (test env '%nowarn expr)
			  (testopt opts 'nowarn expr)
			  (testopt opts 'nowarn #t))))
		((not srcenv)
		 (codewarning (cons* 'UNBOUND expr bound))
		 (when env
		   (add! env '%warnings (cons* 'UNBOUND expr bound)))
		 (when (or optwarn (not env))
		   (warning "The symbol " expr
			    " appears to be unbound given bindings "
			    (apply append bound)))
		 expr)
		((not module) expr)
		((%test srcenv dont-touch-decls expr) expr)
		((%test srcenv '%constants expr)
		 (let ((v (%get srcenv expr)))
		   (if (or (pair? v) (symbol? v) (ambiguous? v))
		       (list 'quote (qc v))
		       v)))
		((not module) expr)
		;; TODO: add 'modrefs' which resolves module.var to a
		;; fcnref and uses that for the symbol
		(else `(,(try (tryif use-opcodes #OP_SYMREF)
			      (tryif (getopt opts 'fcnrefs fcnrefs-default)
				(force-fcnid %modref))
			      %modref)
			,module ,expr)))))))

(define (do-rewrite rewriter expr env bound opts)
  (onerror
      (optimize (rewriter expr) env bound opts)
      (lambda (ex)
	(logwarn |RewriteError| 
	  "Error rewriting " expr " with " rewriter)
	(logwarn |RewriteError| "Error rewriting " expr ": " ex)
	expr)))

(define (check-arguments value n-exprs expr)
  (when (and (procedure-min-arity value)
	     (< n-exprs (procedure-min-arity value)))
    (codewarning (list 'TOOFEWARGS expr value))
    (when optwarn
      (warning "The call to " expr
	       " provides too few arguments "
	       "(" n-exprs ") for " value)))
  (when (and (procedure-arity value)
	     (> n-exprs (procedure-arity value)))
    (codewarning (list 'TOOMANYARGS expr value))
    (when optwarn
      (warning "The call to " expr " provides too many "
	       "arguments (" n-exprs ") for " value))))

(define (optimize-expr expr env bound opts)
  (let* ((head (get-arg expr 0))
	 (use-opcodes (use-opcodes? opts))
	 (n-exprs (-1+ (length expr)))
	 (value (if (symbol? head)
		    (or (get-lexref head bound 0)
			(get env head))
		    head))
	 (from (and (symbol? head)
		    (not (get-lexref head bound 0))
		    (wherefrom head env))))
    (when (and from (module? env))
      (add! env '%symrefs expr)
      (when (and from (table? from))
	(add! env '%modrefs
	      (pick (get from '%moduleid) symbol?))))
    (cond ((or (and from (test from dont-touch-decls head))
	       (and env (test env dont-touch-decls head)))
	   expr)
	  ((and from (%test from '%rewrite)
		(%test (get from '%rewrite) head))
	   (do-rewrite (get (get from '%rewrite) head) 
		       expr env bound opts))
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
	   (loginfo |SpecialForm| "Optimizing reference to " value)
	   (let* ((optimizer
		   (try (get special-form-optimizers value)
			(get special-form-optimizers
			     (procedure-name value))
			(get special-form-optimizers head)))
		  (transformed (try (optimizer value expr env bound opts)
				    expr))
		  (newhead (car transformed)))
	     (cons (try (get opcode-map newhead)
			(tryif (getopt opts 'fcnrefs fcnrefs-default)
			  (get-fcnid head #f newhead))
			newhead)
		   (cdr transformed))))
	  ((exists macro? value)
	   (optimize (macroexpand value expr) env bound opts))
	  ((fail? (reject value applicable?))
	   ;; If all of the head values are applicable, we optimize
	   ;;  the call, replacing the head with shortcuts to the
	   ;;  value
	   (check-arguments value n-exprs expr)
	   (try (tryif (singleton? (get procedure-optimizers value))
		  ((get procedure-optimizers value)
		   value expr env bound opts))
		(callcons
		 (cond ((or (not from) (fail? value)
			    (test from '%nosubst head))
			head)
		       ((test from '%volatile head)
			`(#OP_SYMREF ,from ,head))
		       (else (fcnref value head env opts)))
		 (optimize-args (cdr expr) env bound opts))))
	  (else
	   (when (and optwarn from
		      (not (test from '{%nosubst %volatile} head)))
	     (codewarning (cons* 'NOTFCN expr value))
	     (warning "The current value of " expr " (" head ") "
		      "doesn't appear to be a applicable given "
		      (apply append bound)))
	   expr))))

(define (callcons head tail)
  (cons head (->list tail)))

(defambda (optimize-call expr env bound opts)
  (if (pair? expr)
      (if (or (symbol? (car expr)) (pair? (car expr))
	      (ambiguous? (car expr)))
	  `(,(optimize (car expr) env bound opts)
	    ,@(if (pair? (cdr expr))
		  (optimize-args (cdr expr) env bound opts)
		  (cdr expr)))
	  (optimize-args expr env bound opts))
      expr))

(defambda (optimize-args expr env bound opts)
  (forseq (arg expr)
    (if (or (qchoice? arg) (fail? arg))
	arg
	(optimize arg env bound opts))))

(define (optimize-procedure! proc (opts #f))
  (threadset! 'codewarnings #{})
  (unless (reflect/get proc 'optimized)
    (onerror
	(let* ((env (procedure-env proc))
	       (arglist (procedure-args proc))
	       (body (procedure-body proc))
	       (bound (list (arglist->vars arglist)))
	       (initial (and (pair? body) (car body)))
	       (opts (if (reflect/get proc 'optimize)
			 (if opts
			     (cons (reflect/get proc 'optimize) opts)
			     (reflect/get proc 'optimize))
			 opts))
	       (new-body (optimize-body body env bound opts)))
	  (unless (null? body)
	    (reflect/store! proc 'optimized (gmtimestamp))
	    (reflect/store! proc 'original_args arglist)
	    (reflect/store! proc 'original_body body)
	    (set-procedure-body! proc new-body)
	    (when (pair? arglist)
	      (let ((optimized-args (optimize-arglist arglist env opts)))
		(unless (equal? arglist optimized-args)
		  (set-procedure-args! proc optimized-args))))
	    (if (exists? (threadget 'codewarnings))
		(warning "Errors optimizing " proc ": "
			 (do-choices (warning (threadget 'codewarnings))
			   (printout "\n\t" warning)))
		(lognotice |Optimized| proc))
	    (threadset! 'codewarnings #{})))
	(lambda (ex) 
	  (logwarn |OptimizationError|
	    "While optimizing "
	    (or (procedure-name proc) proc) ", got "
	    (error-condition ex) " in " (error-context ex) 
	    (if (error-details ex) (printout " (" (error-details ex) ")"))
	    (when (error-irritant? ex)
	      (printout "\n" (pprint (error-irritant ex)))))
	  (if persist-default #f ex)))))

(define (optimize-arglist arglist env opts)
  (if (pair? arglist)
      (cons
       (if (and (pair? (car arglist)) (pair? (cdr (car arglist)))
		(singleton? (cadr (car arglist))))
	   `(,(caar arglist) 
	     ,(optimize (cadr (car arglist)) env '() opts))
	   (car arglist))
       (optimize-arglist (cdr arglist) env opts))
      arglist))
  
(define (deoptimize-procedure! proc)
  (when (reflect/get proc 'optimized)
    (reflect/drop! proc 'optimized)
    (when (reflect/get proc 'original_body)
      (set-procedure-body! proc (reflect/get proc 'original_body))
      (reflect/drop! proc 'original_body))
    (when (reflect/get proc 'original_args)
      (set-procedure-args! proc (reflect/get proc 'original_args))
      (reflect/drop! proc 'original_args))))

(define (procedure-optimized? arg)
  (reflect/get arg 'optimized))

(define (optimized? arg)
  (if (compound-procedure? arg)
      (procedure-optimized? arg)
      (and (or (hashtable? arg) (slotmap? arg) (schemap? arg)
	       (environment? arg))
	   (exists procedure-optimized? (get arg (getkeys arg))))))

(define (optimize-get-module spec)
  (onerror (get-module spec)
    (lambda (ex) 
      (irritant+ spec |GetModuleFailed| optimize-module
		 "Couldn't load module " spec))))

(define (optimize-module! module (opts #f))
  (loginfo |OptimizeModule| module)
  (when (symbol? module)
    (set! module (optimize-get-module module)))
  (set! opts
	(if opts
	    (try (cons (get module '%optimize_options) opts)
		 opts)
	    (try (get module '%optimize_options) #f)))
  (let ((bindings (module-bindings module))
	(usefcnrefs (getopt opts 'fcnrefs fcnrefs-default))
	(count 0))
    (do-choices (var bindings)
      (let ((value (get module var)))
	(when (and (exists? value) (compound-procedure? value))
	  (loginfo |OptimizeModule|
	    "Optimizing procedure " var " in " module)
	  (set! count (1+ count))
	  (optimize-procedure! value opts))
	(when (and usefcnrefs (exists? value) (applicable? value))
	  (update-fcnid! var module value))))
    (when (exists symbol? (get module '%moduleid))
      (let* ((referenced-modules (get module '%modrefs))
	     (used-modules
	      (eval `(within-module
		      ',(pick (get module '%moduleid) symbol?)
		      (,getmodules))))
	     (unused (difference used-modules referenced-modules
				 standard-modules
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

(define (optimize-bindings! bindings (opts #f))
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
	      (optimize-procedure! value #f))
	    (warning var " is unbound"))))
    count))

(define (deoptimize-bindings! bindings (opts #f))
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
	      (forseq (x (cdr expr))
		(if (module-arg? x)
		    `(,optimize-get-module ,x)
		    x))))))

(define deoptimize!
  (macro expr
    (if (null? (cdr expr))
	`(,deoptimize-bindings! (,%bindings))
	(cons deoptimize*!
	      (forseq (x (cdr expr))
		(if (module-arg? x)
		    `(,optimize-get-module ,x)
		    x))))))

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

;;; Procedure optimizers

(define (optimize-compound-ref proc expr env bound opts
			       (off-arg) (type-arg))
  (set! off-arg (get-arg expr 2 #f))
  (set! type-arg (get-arg expr 3 #f))
  (tryif (fixnum? off-arg)
    (tryif (and (pair? type-arg) (= (length type-arg) 2)
		(overlaps? (car type-arg) {'quote quote}))
      `(,xref-opcode ,(optimize (get-arg expr 1) env bound opts)
		     ,off-arg ,(cadr type-arg)))
    (tryif (and (pair? type-arg) (eq? (car type-arg) #OP_QUOTE))
      `(,xref-opcode ,(optimize (get-arg expr 1) env bound opts)
		     ,off-arg ,(cadr type-arg)))
    (tryif (and (pair? type-arg) (not type-arg))
      `(,xref-opcode ,(optimize (get-arg expr 1) env bound opts)
		     ,off-arg))))

(store! procedure-optimizers compound-ref optimize-compound-ref)

;;;; Special form handlers

(define (optimize-block handler expr env bound opts)
  (cons (map-opcode handler opts (length (cdr expr)))
	(optimize-body (cdr expr))))

(define (optimize-quote handler expr env bound opts)
  (if (use-opcodes? opts)
      (list #OP_QUOTE (get-arg expr 1))
      expr))

(define (optimize-begin handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons #OP_BEGIN (optimize-body (cdr expr)))
      (optimize-block handler expr env bound opts)))

(define (optimize-if handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons #OP_BRANCH (optimize-body (cdr expr)))
      (optimize-block handler expr env bound opts)))

(define (optimize-not handler expr env bound opts)
  (if (use-opcodes? opts)
      `(#OP_NOT ,(optimize (get-arg expr 1) env bound opts))
      (optimize-block handler expr env bound opts)))

(define (optimize-when handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BRANCH
	,(optimize (cadr expr) env bound opts)
	(#OP_BEGIN ,@(optimize-body (cddr expr))
	 (,void-opcode)))
      (optimize-block handler expr env bound opts)))
(define (optimize-unless handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BRANCH
	(,not-opcode ,(optimize (cadr expr) env bound opts))
	(#OP_BEGIN ,@(optimize-body (cddr expr)) (,void-opcode)))
      (optimize-block handler expr env bound opts)))

(define (optimize-until handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BEGIN
	(,#OP_UNTIL
	 ,(optimize (cadr expr) env bound opts)
	 ,@(optimize-body (cddr expr)))
	(,void-opcode))
      (optimize-block handler expr env bound opts)))
(define (optimize-while handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BEGIN
	(,#OP_UNTIL
	 (,not-opcode ,(optimize (cadr expr) env bound opts))
	 ,@(optimize-body (cddr expr)))
	(,void-opcode))
      (optimize-block handler expr env bound opts)))

(define (optimize-let handler expr env bound opts)
  (if (and (use-opcodes? opts) (use-bindops? opts))
      (let* ((bindexprs (cadr expr))
	     (vars (map car bindexprs))
	     (vals (->vector (map cadr bindexprs)))
	     (not-vars (forseq (var vars) #f))
	     (outer (cons not-vars bound))
	     (inner (cons vars bound)))
	`(#OP_BIND
	  ,(->vector vars)
	  ,(forseq (valexpr vals)
	     (optimize valexpr env outer opts))
	  ,(forseq (bodyexpr (cddr expr))
	     (optimize bodyexpr env inner opts))))
      (let* ((bindexprs (cadr expr))
	     (new-bindexprs
	      (forseq (x bindexprs) `(,(car x) ,(optimize (cadr x) env bound opts))))
	     (body (cddr expr)))
	`(,handler ,new-bindexprs
		   ,@(let ((bound (cons (map car bindexprs) bound)))
		       (optimize-body body))))))

(define (optimize-doexpression handler expr env bound opts)
  (let ((bindspec (cadr expr)) 
	(body (cddr expr)))
    `(,handler (,(car bindspec)
		,(optimize (cadr bindspec) env bound opts)
		,@(cddr bindspec))
	       ,@(let ((bound
			(if (= (length bindspec) 3)
			    (cons (list (first bindspec) (third bindspec))
				  bound)
			    (cons (list (first bindspec)) bound))))
		   (forseq (b body)
		     (optimize b env bound opts))))))
(define (optimize-do2expression handler expr env bound opts)
  (let ((bindspec (cadr expr)) 
	(body (cddr expr)))
    `(,handler ,(cond ((pair? bindspec)
		       `(,(car bindspec)
			 ,(optimize (cadr bindspec) env bound opts)
			 ,@(cddr bindspec)))
		      ((symbol? bindspec)
		       `(,bindspec
			 ,(optimize bindspec env bound opts)))
		      (else (error 'syntax "Bad do-* expression")))
	       ,@(let ((bound
			(if (symbol? bindspec)
			    (cons (list bindspec) bound)
			    (if (= (length bindspec) 3)
				(cons (list (first bindspec)
					    (third bindspec)) bound)
				(cons (list (first bindspec)) bound)))))
		   (forseq (b body) (optimize b env bound opts))))))

(define (optimize-dosubsets handler expr env bound opts)
  (let ((bindspec (cadr expr)) 
	(body (cddr expr)))
    `(,handler (,(car bindspec)
		,(optimize (cadr bindspec) env bound opts)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 4)
				  (cons (list (first bindspec) (fourth bindspec))
					bound)
				  (cons (list (first bindspec)) bound))))
		   (optimize-body body)))))

(define (optimize-let*-bindings bindings env bound opts)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(optimize (cadr (car bindings)) env bound opts))
	,@(optimize-let*-bindings
	   (cdr bindings) env
	   (cons (append (car bound) (list (car (car bindings))))
		 (cdr bound))
	   opts))))

(define (optimize-let* handler expr env bound opts)
  (if (and (use-opcodes? opts) (use-bindops? opts))
      (let* ((bindexprs (cadr expr))
	     (vars (map car bindexprs))
	     (vals (->vector (map cadr bindexprs)))
	     (not-vars (forseq (var vars) #f))
	     (outer (cons not-vars bound))
	     (inner (cons vars bound)))
	`(#OP_BIND
	  ,(->vector vars)
	  ,(forseq (valexpr vals i)
	     (optimize valexpr env 
		       (cons (append (slice vars 0 i)
				     (slice not-vars i))
			     bound)
		       opts))
	  ,(forseq (bodyexpr (cddr expr))
	     (optimize bodyexpr env inner opts))))
      (let ((bindspec (cadr expr))
	    (body (cddr expr)))
	`(,handler
	  ,(optimize-let*-bindings (cadr expr) env (cons '() bound) opts)
	  ,@(let ((bound (cons (map car bindspec) bound)))
	      (optimize-body body))))))

#|
(define (foo x y) (let ((x2 (* x x)) (y2 (* y y))) (+ x2 y2)))
(define (foo* x y)
  (let* ((x (begin (%watch x) (* x x))) 
	 (y (begin (%watch x y)) (* x y)))
    (+ x y)))
|#

(define (optimize-assign handler expr env bound opts)
  (let ((var (get-arg expr 1))
	(setval (get-arg expr 2)))
    (let ((loc (or (get-lexref var bound 0) 
		   (if (wherefrom var env)
		       (cons var (wherefrom var env))
		       var)))
	  (optval (optimize setval  env bound opts)))
      (if (and (use-opcodes? opts) (rewrite? opts))
	  (cond ((symbol? loc) 
		 ;; If loc is a symbol, we couldn't resolve it to a
		 ;; lexical contour or enviroment
		 `(,handler ,var ,optval))
		((overlaps? handler set!) `
		 (#OP_ASSIGN ,loc ,optval #f))
		((overlaps? handler set+!)
		 `(#OP_ASSIGN ,loc ,optval #OP_UNION))
		((overlaps? handler default!) 
		 `(#OP_ASSIGN ,loc ,optval  #t))
		(else `(,handler ,var ,optval)))
	  `(,handler ,var ,optval)))))

(define (optimize-lambda handler expr env bound opts)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (cons (arglist->vars (cadr expr)) bound))
		     (body (cddr expr)))
		 (forseq (b body)
		   (optimize b env bound opts)))))

(define (old-optimize-cond handler expr env bound opts)
  (cons handler 
	(forseq (clause (cdr expr))
	  (cond ((eq? (car clause) 'else)
		 `(ELSE ,@(optimize-body (cdr clause))))
		((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
		 `(,(optimize (car clause) env bound opts)
		   =>
		   ,@(optimize-body (cdr clause))))
		(else (optimize-body clause))))))

(define (optimize-cond handler expr env bound opts)
  (if (and (use-opcodes? opts)
	   (not (exists? (lambda (clause) 
			   (and (pair? clause) (pair? (cdr clause))
				(identical? (cadr clause) '==>))))))
      (convert-cond (cdr expr) env bound opts)
      (cons handler 
	    (forseq (clause (cdr expr))
	      (cond ((eq? (car clause) 'else)
		     `(ELSE ,@(optimize-body (cdr clause))))
		    ((and (pair? (cdr clause)) 
			  (identical? (cadr clause) '=>))
		     `(,(optimize (car clause) env bound opts)
		       =>
		       ,@(optimize-body (cddr clause))))
		    (else (optimize-body clause)))))))

(define (convert-cond clauses env bound opts)
  (if (null? clauses) (list void-opcode)
      `(#OP_BRANCH 
	,(if (overlaps? (car (car clauses)) '{else default})
	     #t
	     (optimize (car (car clauses)) env bound opts))
	(#OP_BEGIN 
	 ,@(forseq (c (cdr (car clauses)))
	     (optimize c env bound opts)))
	,(convert-cond (cdr clauses) env bound opts))))

(define (optimize-and handler expr env bound opts)
  (if (use-opcodes? opts)
      `(#OP_AND ,@(optimize-body (cdr expr)))
      (optimize-block handler expr env bound opts)))

(define (optimize-or handler expr env bound opts)
  (if (use-opcodes? opts)
      `(#OP_OR ,@(optimize-body (cdr expr)))
      (optimize-block handler expr env bound opts)))

(define (optimize-try handler expr env bound opts)
  (if (use-opcodes? opts)
      `(#OP_TRY ,@(optimize-body (cdr expr)))
      (optimize-block handler expr env bound opts)))

(define (optimize-tryif handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BRANCH 
	,(optimize (get-arg expr 1) env bound opts)
	(#OP_TRY ,@(optimize-body (cddr expr)))
	{})
      (optimize-block handler expr env bound opts)))

(define (optimize-case handler expr env bound opts)
  `(,handler 
    ,(optimize (cadr expr) env bound opts)
    ,@(forseq (clause (cddr expr))
	(cons (car clause)
	      (forseq (x (cdr clause))
		(optimize x env bound opts))))))

(define (optimize-unwind-protect handler expr env bound opts)
  `(,handler ,(optimize (cadr expr) env bound opts)
	     ,@(optimize-body (cddr expr))))

(define (optimize-quasiquote handler expr env bound opts)
  `(,handler ,(optimize-quasiquote-node (cadr expr) env bound opts)))
(defambda (optimize-quasiquote-node expr env bound opts)
  (cond ((ambiguous? expr)
	 (for-choices (elt expr)
	   (optimize-quasiquote-node elt env bound opts)))
	((and (pair? expr) 
	      (or (eq? (car expr) 'unquote)  (eq? (car expr) 'unquote*)))
	 `(,(car expr) ,(optimize (cadr expr) env bound opts)))
	((pair? expr)
	 (forseq (elt expr)
	   (optimize-quasiquote-node elt env bound opts)))
	((vector? expr)
	 (forseq (elt expr)
	   (optimize-quasiquote-node elt env bound opts)))
	((slotmap? expr)
	 (let ((copy (frame-create #f))
	       (slots (getkeys expr)))
	   (do-choices (slot slots)
	     (store! copy (optimize-quasiquote-node slot env bound opts)
		     (try (optimize-quasiquote-node (get expr slot) env bound opts)
			  (get expr slot))))
	   copy))
	(else expr)))

(define (optimize-logmsg handler expr env bound opts)
  (if (or (symbol? (cadr expr)) (number? (cadr expr)))
      (if (or (symbol? (caddr expr)) (number? (caddr expr)))
	  `(,handler ,(cadr expr)
		     ,(caddr expr)
		     ,@(optimize-body (cdddr expr)))
	  `(,handler ,(cadr expr) ,@(optimize-body (cddr expr))))
      `(,handler ,@(optimize-body (cdr expr)))))

(define (optimize-logif handler expr env bound opts)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      `(,handler ,(optimize (cadr expr) env bound opts)
		 ,(caddr expr)
		 ,@(optimize-body (cdddr expr)))
      `(,handler ,(optimize (cadr expr) env bound opts)
		 ,@(optimize-body (cddr expr)))))

(define (optimize-logif+ handler expr env bound opts)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      (if (or (symbol? (cadr (cddr expr))) (number? (cadr (cddr expr))))
	  `(,handler ,(optimize (cadr expr) env bound opts)
		     ,(caddr expr)
		     ,(cadr (cddr expr))
		     ,@(optimize-body (cdr (cdddr expr))))
	  `(,handler ,(optimize (cadr expr) env bound opts)
		     ,(caddr expr)
		     ,@(optimize-body (cdddr expr))))
      `(,handler ,(optimize (cadr expr) env bound opts)
		 ,@(optimize-body (cddr expr)))))

;;; Optimizing XHTML expressions

;; This doesn't handle mixed alist ((x y)) and plist (x y) attribute lists
;;  because they shouldn't work anyway
(define (optimize-attribs attribs env bound opts)
  (cond ((not (pair? attribs)) attribs)
	((pair? (cdr attribs))
	 `(,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		`(,(car (car attribs))
		  ,(optimize (cadr (car attribs)) env bound opts))
		(car attribs))
	   ,(if (and (pair? (car attribs))
		     (not (eq? (car (car attribs)) 'quote)))
		(if (pair? (cadr attribs))
		    `(,(car (cadr attribs))
		      ,(optimize (cadr (cadr attribs)) env bound opts))
		    (optimize (cadr attribs) env bound opts))
		(optimize (cadr attribs) env bound opts))
	   ,@(optimize-attribs (cddr attribs) env bound opts)))
	((pair? (car attribs))
	 `((,(car (car attribs))
	    ,(optimize (cadr (car attribs)) env bound opts))))
	(else attribs)))

(define (optimize-markup handler expr env bound opts)
  `(,(car expr) ,@(optimize-body (cdr expr))))
(define (optimize-markup* handler expr env bound opts)
  `(,(car expr) ,(optimize-attribs (second expr) env bound opts)
    ,@(optimize-body (cddr expr))))

(define (optimize-emptymarkup fcn expr env bound opts)
  `(,(car expr) ,@(optimize-attribs (cdr expr) env bound opts)))

(define (optimize-anchor* fcn expr env bound opts)
  `(,(car expr) ,(optimize (cadr expr) env bound opts)
    ,(optimize-attribs (third expr) env bound opts)
    ,@(optimize-body (cdddr expr))))

(define (optimize-xmlblock fcn expr env bound opts)
  `(,(car expr) ,(cadr expr)
    ,(optimize-attribs (third expr) env bound opts)
    ,@(optimize-body (cdddr expr))))

;;; Declare them

(add! special-form-optimizers let optimize-let)
(add! special-form-optimizers let* optimize-let*)
(when (bound? letq)
  (add! special-form-optimizers letq optimize-let)
  (add! special-form-optimizers letq* optimize-let*))
(add! special-form-optimizers (choice lambda ambda slambda)
      optimize-lambda)
(add! special-form-optimizers 
      (choice set! set+! default! define)
      optimize-assign)

(add! special-form-optimizers
      (choice dolist dotimes doseq forseq)
      optimize-doexpression)
(add! special-form-optimizers
      (choice do-choices for-choices filter-choices try-choices)
      optimize-do2expression)

(add! special-form-optimizers tryif optimize-tryif)
(add! special-form-optimizers and optimize-and)
(add! special-form-optimizers or optimize-or)
(add! special-form-optimizers try optimize-try)

(add! special-form-optimizers begin optimize-begin)
(add! special-form-optimizers if optimize-if)
(add! special-form-optimizers when optimize-when)
(add! special-form-optimizers unless optimize-unless)

(add! special-form-optimizers while optimize-while)
(add! special-form-optimizers until optimize-until)

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
