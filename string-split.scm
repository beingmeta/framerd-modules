(in-module 'string-split)

(module-export! '{string-split})

;;; Bind some function aliases
(define string-length length)
(define substring subseq)
(define add1 1+)
(define sub1 1-)

;;; Return true if STRING is an empty string
(define (string-null? string)
  (if (zero? (string-length string))
      #t
      #f))

;;; Return a list from STRING
(define (string->list string)
  (->list string))

;;; Return element from XS indexed by COUNT
(define (list-ref xs count)
  (cond ((null? xs) #f)
        ((zero? count) (car xs))
        (else (list-ref (cdr xs) (sub1 count)))))

;;; Return element from STRING indexd by REF
(define (string-ref string ref)
  (let ((xs (string->list string)))
    (list-ref xs ref)))

;;; Return a list by splitting STRING by CHAR
(define (split string char len a b)
  (cond ((>= b len)
         (if (= a b)
             '()
             (cons (substring string a b) '())))
        ((char=? char (string-ref string b))
         (if (= a b)
             (split string char len (+ 1 a) (+ 1 b))
             (cons (substring string a b) (split string char len b b))))
        (else (split string char len a (+ 1 b)))))

;;; Split STRING by CHAR
(define (string-split string char)
  (let ((len (string-length string)))
    (split string char len 0 0)))
