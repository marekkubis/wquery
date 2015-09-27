if exists("b:current_syntax")
    finish
endif

" - is allowed in keywords
setlocal iskeyword+=_

syn case match

syn match Comment "#.*$" contains=@Spell

syn match Identifier "\$\<[a-zA-Z_][a-zA-Z0-9_]*\>"
syn match Identifier "@\<[a-zA-Z_][a-zA-Z0-9_]*\>"
syn match Identifier "%\<[a-zA-Z_][a-zA-Z0-9_]*\>"

syn match Number "\<\d\+\>"

syn match Operator "+\|-\|/\|*\|:=\|=\|?\|!\|<\|>\||\|\^\|\.\|\[\|\]"
syn keyword Operator in not and or mod div union except intersect

syn keyword Statement do end from emit if else while function true false

syn region String start=/`/ end=/`/

syn region String start=/"/ end=/"/

syn region String start=/'/ end=/'/

let b:current_syntax = "wquery"
