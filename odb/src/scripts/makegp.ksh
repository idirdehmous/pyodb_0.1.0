#!/bin/ksh

set -eu

bname=$(basename $0)

: ${terminal:=}
: ${title:=}
: ${size:=}
: ${origin:=}
: ${rotate_x:=60}
: ${rotate_z:=30}
: ${scale_x:=1}
: ${scale_z:=1}
: ${xdata:=}
: ${ydata:=}
: ${zdata:=}
: ${timefmt:=}
: ${xlabel:=}
: ${formatx:=}
: ${minx:=}
: ${maxx:=}
: ${ylabel:=}
: ${formaty:=}
: ${miny:=}
: ${maxy:=}
: ${zlabel:=}
: ${formatz:=}
: ${minz:=}
: ${maxz:=}
: ${key:=}
: ${plotting:=plot}
: ${usings:=}
: ${items:=}
: ${styles:=}
: ${lw:=}
: ${boxwidth:=}
: ${regexp:='\(.*\)'}
: ${postload:=}

function Usage
{
cat 1>&2 <<EOF
Usage: $(basename $0) files
       [--title=TITLE]
       [--size=SIZE]
       [--origin=ORIGIN]
       [--rotate_x=ROTATE_X]
       [--rotate_z=ROTATE_Z]
       [--scale_x=SCALE_X]
       [--scale_z=SCALE_Z]
       [--xdata=XDATA]
       [--ydata=YDATA]
       [--timefmt=TIMEFMT]
       [--xlabel=X LABEL]
       [--formatx=FORMAT OF X]
       [--minx=MINIMUM OF X]
       [--maxx=MAXIMUM OF X]
       [--ylabel=Y LABEL]
       [--formaty=FORMAT OF Y]
       [--miny=MINIMUM OF Y]
       [--maxy=MAXIMUM OF Y]
       [--zlabel=Z LABEL]
       [--formatz=FORMAT OF Z]
       [--minz=MINIMUM OF Z]
       [--maxz=MAXIMUM OF Z]
       [--key=KEY]
       [--plotting=PLOTTING (defalut ${plotting})]
       [--usings=USINGS]
       [--items=ITEMS]
       [--styles=STYLES]
       [--lw=LINESWIDTH]
       [--boxwidth=BOXWIDTH]
       [--regexp=REGEXP (default ${regexp})]
EOF

exit 1
}

while [ $# -gt 0 ]
do
    case $1 in
        --vname=*) vname="${vname:+${vname} }$(echo "$1" | sed -e's/--vname=//')" ;;
        --boxwidth=* | --formatx=* | --formaty=* | --formatz=* | --key=* | --maxx=* | --maxy=* | --maxz=* | --minx=* | --miny=* | --minz=* | --origin=* | --plotting=* | --regexp=* | --rotate_x=* | --rotate_z=* | --scale_x=* | --scale_z=* | --size=* | --terminal=* | --timefmt=* | --title=* | --xdata=* | --xlabel=* | --ydata=* | --ylabel=* | --zdata=* | --zlabel=*)
            vname=$(echo "$1" | sed -e's/^--//' -e's/=.*$//')
            eval ${vname}=\"$(echo "$1" | sed -e"s/^--${vname}=//" | awk '{printf("%s",$0)}')\"
            ;;
        --lw=* | --postload=* | --styles=* | --usings=*)
            vname=$(echo "$1" | sed -e's/^--//' -e's/=.*$//')
            eval ${vname}=\"\${${vname}:+\${${vname}} }$(echo "$1" | sed -e"s/^--${vname}=//" | awk '{printf("%s",$0)}')\"
            ;;
        --items=*)
            vname=$(echo "$1" | sed -e's/^--//' -e's/=.*$//')
            eval ${vname}=\"\${${vname}:+\${${vname}},}$(echo "$1" | sed -e"s/^--${vname}=//" | awk '{printf("%s",$0)}')\"
            ;;
        --*)
            echo ${bname}: illegal option $1 1>&2
            Usage
            ;;
	*)  break ;;
    esac

    shift
done

: ${*:?}

if [ -z "${usings}" ]; then
    if [ "${plotting}" = splot ]; then
        usings=1:2:3
    else
        case ${styles} in
            xyerrorbars) usings=1:2:3:4:5:6 ;;
            yerrorbars) usings=1:2:3:4 ;;
            errorbars) usings=1:2:3 ;;
            *)  usings=1:2 ;;
        esac
    fi
fi

cbuf=${styles}
styles=
field=1
for using in ${usings}
do
    style=$(echo ${cbuf} | awk -v field=${field} '{print $field}')
    if [ -n "${style}" ];then
        styles="${styles:+${styles} }${style}"
    else
        if [ "${plotting}" = splot ];then
            styles="${styles:+${styles} }linespoints"
        else
            case $(echo "${using}" | awk -F: '{print NF}') in
                6) styles="${styles:+${styles} }xyerrorbars" ;;
                4) styles="${styles:+${styles} }yerrorbars" ;;
                3) styles="${styles:+${styles} }errorbars" ;;
                *) styles="${styles:+${styles} }points" ;;
            esac
        fi
    fi
    ((field+=1))
done

if [ -n "${terminal}" ]; then
    case ${terminal} in
        *) echo set terminal ${terminal} ;;
    esac
fi

for vname in boxwidth origin size xdata ydata zdata
do
    if [ -n "`eval echo \\${${vname}}`" ]; then
        eval echo set ${vname} \${${vname}}
    fi
done

for vname in output timefmt title xlabel ylabel zlabel
do
    if [ -n "`eval echo \\${${vname}}`" ]; then
        eval echo set ${vname} '\"'\${${vname}}'\"'
    fi
done

test -n "${formatx}" && echo set format x \"${formatx}\"
test -n "${formaty}" && echo set format y \"${formaty}\"
test -n "${formatz}" && echo set format z \"${formatz}\"

if [ "${plotting}" = splot ]; then
    cat <<EOF
rotate_x=${rotate_x}
rotate_z=${rotate_z}
scale_x=${scale_x}
scale_z=${scale_z}
set view rotate_x, rotate_z, scale_x, scale_z
EOF
fi

if [ "${key}" = nokey ];then
    echo set ${key}
elif [ -n "${key}" ];then
    echo set key ${key}
fi    

if [ "${xdata}" = time ]; then
    test -n "${minx}" && minx=\"${minx}\"
    test -n "${maxx}" && maxx=\"${maxx}\"
fi

if [ "${ydata}" = time ]; then
    test -n "${miny}" && miny=\"${miny}\"
    test -n "${maxy}" && maxy=\"${maxy}\"
fi

if [ "${zdata}" = time ]; then
    test -n "${minz}" && minz=\"${minz}\"
    test -n "${maxz}" && maxz=\"${maxz}\"
fi

nusing=$(echo ${usings} | awk '{print NF}')

if [ "${plotting}" = splot ];then
    echo ${plotting} [ ${minx} : ${maxx} ] [ ${miny} : ${maxy} ] [ ${minz} : ${maxz} ] \\
else
    echo ${plotting} [ ${minx} : ${maxx} ] [ ${miny} : ${maxy} ] \\
fi

i=1
while [ $# -gt 0 ]
do
    fname=$1

    if [ ${nusing} -gt 1 ];then
        group=
    else
        case ${regexp} in
            [0-9]*) 
                group=$(echo ${fname} | awk -v field=${regexp} '{print $field}')
                ;;
            *)  group=$(expr "${fname}" : ${regexp}) || true ;;
        esac
    fi

    field=1
    for using in ${usings}
    do
        if [ ${nusing} -le 1 ];then
            item=
        else
            item=$(echo ${items} | awk -F , -v field=${field} '{print $field}')
        fi
        item="${group}${item:+ ${item}}"

        style=$(echo ${styles} | awk -v field=${field} '{print $field}')
        with="${style:+${style}}${lw:+ lw ${lw}}"

        if [ ${i} -eq 1 ] && [ ${field} -eq 1 ];then
            :
        else
            echo ',\'
        fi

        echo " \"${fname}\" ${using:+using ${using}} ${item:+title \"${item}\"} ${with:+with ${with}}" | awk '{printf("%s",$0)}'

	((field+=1))
    done

    ((i+=1))

    shift
done

echo

for fname in ${postload}
do
    echo load \"${fname}\"
done

exit 0
