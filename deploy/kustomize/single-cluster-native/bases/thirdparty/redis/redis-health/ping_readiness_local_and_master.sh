script_dir="$(dirname "$0")"
exit_status=0
"$script_dir/ping_readiness_local.sh" $1 || exit_status=$?
"$script_dir/ping_readiness_master.sh" $1 || exit_status=$?
exit $exit_status