#!/usr/bin/gnuplot
#
# latency timeline between two nodes
# assumes input in minutes, latency in milliseconds
reset

# set color of borders to gray
set style line 11 lc rgb '#808080' lt 1
set border back ls 11
set tics nomirror

# line style definitions
set style line 1 lc rgb "red" pt 1 ps 1 lt 1 lw 1
set style line 2 lc rgb "orange" pt 1 ps 1 lt 1 lw 1

# define grid to be only at x-axis and gray with dotted lines
set style line 12 lc rgb '#808080' lt 0 lw 2
set grid xtics ls 12

set autoscale               # scale axes automatically
unset log                   # remove any log-scaling
unset label                 # remove any previous labels
set xtic auto                 # set xtics
set ytic auto               # set ytics automatically
set key font ",8"

set xlabel "Time in minutes"
set ylabel "Latency in msec"
set terminal pdf size 10,5
set output "latency_timeline.pdf"

# standard plot
plot "tmp.dat" using ($1):($2) title 'latency' with lines ls 1,\
    "tmp.dat" using ($1):($3) title 'average latency' with lines ls 2
