(in-module 'compat/strings)

(module-export! '{string-length
                  substring
                  string-null?

                  string->list
                  string-ref
                  string-split

                  string-trim-left
                  string-trim-right
                  string-trim
                  unescape-string})

;;; Bind some function aliases
(define-if-needed string-length length)
(define-if-needed substring subseq)
(define-if-needed string-null? empty-string?)
(define-if-needed add1 1+)
(define-if-needed sub1 1-)
(define-if-needed string->list ->list)

;;; Return element from XS indexed by COUNT
(define-if-needed list-ref elt)
(define-if-needed string-ref elt)

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
(define (string-split string (char #\space))
  (let ((len (string-length string)))
    (split string char len 0 0)))

;;; Remove leading items matching ITEM from XS
(define (trim-left xs item)
  (cond ((eqv? (car xs) item) (trim-left (cdr xs) item))
        (else xs)))

;;; Remove trailing items matching ITEM from XS
(define (trim-right xs item)
  (reverse (trim-left (reverse xs) item)))

;;; Remove leading CHAR from STRING
(define (string-trim-left string (char #\space))
  (let ((xs (->list string)))
    (->string (trim-left xs char))))

;;; Remove trailing CHAR from STRING
(define (string-trim-right string (char #\space))
  (let ((xs (->list string)))
    (->string (trim-right xs char))))

;;; Remove leading and trailing CHAR from STRING
(define (string-trim string (char #\space))
  (string-trim-left (string-trim-right string char) char))

;;; Return true if string is double quote-escaped
(define (escaped-string? string)
  (let ((quote "\""))
    (and (has-prefix string quote) (has-suffix string quote))))

;;; Remove leading and trailing escaped quotes
(define (unescape-string string)
  (if (escaped-string? string)
      (string-trim string #\")
      string))
