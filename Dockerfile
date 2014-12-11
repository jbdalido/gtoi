FROM progrium/busybox

COPY gtoi /usr/bin/gtoi

EXPOSE 9666

ENTRYPOINT ["/usr/bin/gtoi"]