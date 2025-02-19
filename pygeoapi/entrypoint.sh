#!/bin/bash
echo "START /entrypoint.sh"

set +e

# pygeoapi settings with defaults and configurable via env
PYGEOAPI_HOME=${PYGEOAPI_HOME:=/pygeoapi}
export PYGEOAPI_HOME

PYGEOAPI_CONFIG=${PYGEOAPI_CONFIG:=${PYGEOAPI_HOME}/local.config.yml}
export PYGEOAPI_CONFIG

PYGEOAPI_OPENAPI=${PYGEOAPI_OPENAPI:=${PYGEOAPI_HOME}/local.openapi.yml}
export PYGEOAPI_OPENAPI

# gunicorn env settings with defaults
SCRIPT_NAME=${SCRIPT_NAME:=/}
CONTAINER_NAME=${CONTAINER_NAME:=pygeoapi}
CONTAINER_HOST=${CONTAINER_HOST:=0.0.0.0}
CONTAINER_PORT=${CONTAINER_PORT:=80}
WSGI_WORKERS=${WSGI_WORKERS:=4}
WSGI_WORKER_TIMEOUT=${WSGI_WORKER_TIMEOUT:=6000}
WSGI_WORKER_CLASS=${WSGI_WORKER_CLASS:=gevent}

# Add the pygeoapi directory to the Python path
export PYTHONPATH="${PYGEOAPI_HOME}/src:${PYTHONPATH}"

# What to invoke: default is to run gunicorn server
entry_cmd=${1:-run}

# Shorthand
function error() {
	echo "ERROR: $*"
	exit 1
}

# Workdir
cd "${PYGEOAPI_HOME}" || exit 1

echo "Trying to generate ${PYGEOAPI_OPENAPI}"
pygeoapi openapi generate "${PYGEOAPI_CONFIG}" --output-file "${PYGEOAPI_OPENAPI}"

# shellcheck disable=SC2181
[[ $? -ne 0 ]] && error "${PYGEOAPI_OPENAPI} could not be generated ERROR"

echo "${PYGEOAPI_OPENAPI} generated continue to pygeoapi"

case ${entry_cmd} in
	# Run Unit tests
	test)
	  for test_py in tests/test_*.py
	  do
	  	# handle the case of no test files
	  	[[ -e "$test_py" ]] || break
	    # Skip tests requireing backend server or libs installed
	    case ${test_py} in
	        tests/test_elasticsearch__provider.py)
	        	;&
	        tests/test_sensorthings_provider.py)
	        	;&
	        tests/test_postgresql_provider.py)
			    ;&
	        tests/test_mongo_provider.py)
	        	echo "Skipping: ${test_py}"
	        ;;
	        *)
	        	python3 -m pytest "${test_py}"
	         ;;
	    esac
	  done
	  ;;

	# Run pygeoapi server
	#
	# https://docs.pygeoapi.io/en/latest/running.html#gunicorn-and-flask
	#
	# https://docs.gunicorn.org/en/stable/settings.html
	#
	# FIXME: setting "--reload + --reload-extra-file intended for development/debugging only!
	run)
		# SCRIPT_NAME should not have value '/'
		[[ "${SCRIPT_NAME}" = '/' ]] && export SCRIPT_NAME="" && echo "make SCRIPT_NAME empty from /"

		echo "Start gunicorn name=${CONTAINER_NAME} on ${CONTAINER_HOST}:${CONTAINER_PORT} with ${WSGI_WORKERS} workers and SCRIPT_NAME=${SCRIPT_NAME}"
		exec gunicorn --workers "${WSGI_WORKERS}" \
				--worker-class="${WSGI_WORKER_CLASS}" \
				--timeout "${WSGI_WORKER_TIMEOUT}" \
				--name="${CONTAINER_NAME}" \
				--bind "${CONTAINER_HOST}:${CONTAINER_PORT}" \
				--reload \
    			--reload-extra-file "${PYGEOAPI_CONFIG}" \
				pygeoapi.flask_app:APP
	  ;;
	*)
	  error "unknown command arg: must be run (default) or test"
	  ;;
esac

echo "END /entrypoint.sh"
