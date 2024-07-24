#!/bin/bash

set -ue

script=${0//*\//}
tool=$(echo $script | sed 's/\(odb_\|odb\)//')

case $tool in
    to_request)
        usage="odb oda2request $@"
        ;;
    dump)
        usage="odb sql \* -i $@"
        ;;
    count|header|set|split)
        usage="odb $tool $@"
        ;;
    *)
        usage="odb$tool $@"
        ;;
esac

echo "error: Wrapper script '$script' has been deprecated. Please use the following command: $usage" 1>&2
exit 1

