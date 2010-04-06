(in-package :tokyo-cabinet)

(defmethod serialize ((value integer) &key (octet-count 4))
  (make-array octet-count 
	      :element-type '(unsigned-byte 8) 
	      :initial-contents (loop for i from 0 below octet-count 
				      collect
				   (ldb (byte 8 (* 8 i)) value))))

(defmethod deserialize ((value vector) &key (atom-size 4) (type 'integer))
  (make-array `(,(/ (length value) atom-size))
	      :element-type type
	      :initial-contents (loop for base below (length value) by atom-size
				      collect
				   (loop for i below atom-size
					 sum 
					 (ash (aref value (+ base i))
					      (* i 8))))))

(defmacro with-fstring ((var string encoder) &body body)
  `(let ((len-str (length ,string)))
     (multiple-value-bind (size end)
	 (funcall (slot-value ,encoder 'oct-counter) ,string 0 len-str
		  (- (1+ len-str) (slot-value ,encoder 'nul-len)))
       (let ((,var (foreign-alloc :char :count size)))
	 (funcall (slot-value db 'encoder) ,string 0 end ,var 0)
	 (dotimes (i (slot-value db 'nul-len))
	   (setf (mem-ref buffer :char (+ size i)) 0))
	 (progn ,@body)))))
  

(defmacro with-fstrings (bindings &body body)
  (if bindings
      `(with-fstring ,(car bindings)
         (with-fstrings ,(cdr bindings)
           ,@body))
      `(progn ,@body)))


(declaim (ftype (function (codec string foreign-pointer integer) 
			  foreign-pointer) 
		cl-str->db-str)
	 (inline cl-str->db-str))
(defun cl-str->db-str (codec string buffer bufsize)
  (check-type string string)
  (assert (plusp bufsize))
  (multiple-value-bind (size end)
      (funcall (oct-counter codec) string 0 (length string) 
	       (- bufsize (nul-len codec)))
    (funcall (encoder codec) string 0 end buffer 0)
    (dotimes (i (nul-len codec))
      (setf (mem-ref buffer :char (+ size i)) 0)))
  buffer)

(defun db-str->cl-str (codec pointer 
		       &key (max-chars (1- array-total-size-limit)))
  (unless (null-pointer-p pointer)
    (flet ((foreign-str-len ()
	     (ecase (nul-len codec)
	       (1 (cffi::%foreign-string-length pointer 0 :uint8 1))
	       (2 (cffi::%foreign-string-length pointer 0 :uint16 2))
	       (4 (cffi::%foreign-string-length pointer 0 :uint32 4)))))
      (let ((count (foreign-str-len)))	    
	(assert (plusp max-chars))
	(multiple-value-bind (size new-end)
	    (funcall (code-point-counter codec)
		     pointer 0 count max-chars)
        (let ((string (make-string size :element-type 'babel:unicode-char)))
          (funcall (decoder codec) pointer 0 new-end string 0)
          (values string new-end)))))))