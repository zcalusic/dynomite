MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS =
if !OS_SOLARIS
AM_CPPFLAGS += -D_GNU_SOURCE
endif
AM_CPPFLAGS += -DSYSCONFDIR="\"$(sysconfdir)\""
AM_CPPFLAGS += -I $(top_srcdir)/src/hashkit
AM_CPPFLAGS += -I $(top_srcdir)/src/proto
AM_CPPFLAGS += -I $(top_srcdir)/src/event
AM_CPPFLAGS += -I $(top_srcdir)/src/seedsprovider

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wpointer-arith
AM_CFLAGS += -Winline
AM_CFLAGS += -Wunused-function -Wunused-variable -Wunused-value
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -Wconversion -Wsign-compare
AM_CFLAGS += -Wstrict-prototypes -Wmissing-prototypes -Wredundant-decls -Wmissing-declarations -Wimplicit-function-declaration

AM_LDFLAGS =
AM_LDFLAGS += -lm -lpthread -rdynamic

if OS_SOLARIS
AM_LDFLAGS += -lnsl -lsocket
endif

SUBDIRS = hashkit proto event seedsprovider tools

sbin_PROGRAMS = dynomite

check_PROGRAMS = dynomite-test
TESTS = dynomite-test

dynomite_SOURCES =			                          \
        dyn_array.c dyn_array.h		                          \
        dyn_cbuf.h                                                \
        dyn_client.c dyn_client.h                                 \
        dyn_conf.c dyn_conf.h		                          \
        dyn_connection.c dyn_connection.h                         \
        dyn_connection_internal.c dyn_connection_internal.h       \
        dyn_connection_pool.c dyn_connection_pool.h               \
        dyn_core.c dyn_core.h                                     \
        dyn_dict_msg_id.h dyn_dict_msg_id.c                       \
        dyn_dnode_client.h dyn_dnode_client.c                     \
        dyn_dnode_msg.c dyn_dnode_msg.h                           \
        dyn_dnode_peer.c  dyn_dnode_peer.h                        \
        dyn_dnode_request.c                                       \
        dyn_dnode_proxy.c dyn_dnode_proxy.h                       \
        dyn_histogram.c dyn_histogram.h                           \
        dyn_proxy.c dyn_proxy.h		                          \
        dyn_message.c dyn_message.h	                          \
        dyn_request.c                                             \
        dyn_response_mgr.c dyn_response_mgr.h                     \
        dyn_response.c			                          \
        dyn_ring_queue.h dyn_ring_queue.c                         \
        dyn_mbuf.c dyn_mbuf.h		                          \
        dyn_node_snitch.c dyn_node_snitch.h                       \
        dyn_rbtree.c dyn_rbtree.h		                  \
        dyn_server.c dyn_server.h		                  \
        dyn_setting.c dyn_setting.h                               \
        dyn_signal.c dyn_signal.h		                  \
        dyn_stats.c dyn_stats.h		                          \
        dyn_string.c dyn_string.h		                  \
        dyn_types.c dyn_types.h                                   \
        dyn_log.c dyn_log.h		                          \
        dyn_util.c dyn_util.h		                          \
        dyn_vnode.c dyn_vnode.h                                   \
        dyn_queue.h			                          \
        dyn_task.h dyn_task.c									  \
        dyn_tls.c dyn_tls.h                                       \
        dyn_gossip.c dyn_gossip.h                                 \
        dyn_dict.c dyn_dict.h                                     \
        dynomite.c

dynomite_LDADD = $(top_builddir)/src/hashkit/libhashkit.a
dynomite_LDADD += $(top_builddir)/src/proto/libproto.a
dynomite_LDADD += $(top_builddir)/src/event/libevent.a
dynomite_LDADD +=  $(top_builddir)/src/seedsprovider/libseedsprovider.a -lresolv



dynomite_test_SOURCES =                                           \
        dyn_cbuf.h                                                \
        dyn_core.c dyn_core.h                                     \
        dyn_connection.c dyn_connection.h                         \
        dyn_connection_internal.c dyn_connection_internal.h		  \
        dyn_connection_pool.c dyn_connection_pool.h               \
        dyn_client.c dyn_client.h                                 \
        dyn_dict_msg_id.h dyn_dict_msg_id.c                       \
        dyn_dnode_client.h dyn_dnode_client.c                     \
        dyn_dnode_msg.c dyn_dnode_msg.h                           \
        dyn_dnode_peer.c  dyn_dnode_peer.h                        \
        dyn_dnode_request.c                                       \
        dyn_dnode_proxy.c dyn_dnode_proxy.h                       \
        dyn_histogram.c dyn_histogram.h                           \
        dyn_server.c dyn_server.h                                 \
        dyn_proxy.c dyn_proxy.h                                   \
        dyn_message.c dyn_message.h                               \
        dyn_request.c                                             \
        dyn_response_mgr.c dyn_response_mgr.h                     \
        dyn_response.c                                            \
        dyn_ring_queue.h dyn_ring_queue.c                         \
        dyn_mbuf.c dyn_mbuf.h                                     \
        dyn_conf.c dyn_conf.h                                     \
        dyn_node_snitch.c dyn_node_snitch.h                       \
        dyn_setting.c dyn_setting.h                               \
        dyn_stats.c dyn_stats.h                                   \
        dyn_signal.c dyn_signal.h                                 \
        dyn_types.c dyn_types.h                                   \
        dyn_rbtree.c dyn_rbtree.h                                 \
        dyn_log.c dyn_log.h                                       \
        dyn_string.c dyn_string.h                                 \
        dyn_array.c dyn_array.h                                   \
        dyn_util.c dyn_util.h                                     \
        dyn_queue.h                                               \
        dyn_task.h dyn_task.c									  \
        dyn_tls.c dyn_tls.h                                       \
        dyn_vnode.c dyn_vnode.h                                   \
        dyn_gossip.c dyn_gossip.h                                 \
        dyn_dict.c dyn_dict.h                                     \
        dyn_test.c


dynomite_test_LDADD = $(top_builddir)/src/hashkit/libhashkit.a
dynomite_test_LDADD += $(top_builddir)/src/proto/libproto.a
dynomite_test_LDADD += $(top_builddir)/src/event/libevent.a
dynomite_test_LDADD +=  $(top_builddir)/src/seedsprovider/libseedsprovider.a -lresolv
