ensure_transpiler_ready:
	python ./ensure_transpiler_ready.py

test: ensure_transpiler_ready
	python ./ensure_tests_transpiled.py `pwd`/test/*.test.grasp

.PHONY: ensure_transpiler_ready