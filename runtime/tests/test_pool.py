"""Task bodies in task processes, through `kabudachi.run()`: where they run,
what crosses the pipe, nested calls, a process that dies, a body past its
hard limit, a cancel, and compaction. Each test starts one or two task
processes, which import their tasks from `pool_tasks`."""

import multiprocessing
import os
import struct

from kabudachi import ipc


def test_a_frame_cut_short_by_a_closed_pipe_ends_reading_after_the_whole_frames():
    reader, writer = multiprocessing.Pipe(duplex=False)
    writer.send(ipc.Exited("whole"))
    # A length prefix promising far more than follows, as when a task
    # process dies while writing a large frame.
    os.write(writer.fileno(), struct.pack("!i", 1_000_000) + b"cut short")
    writer.close()
    delivered = []

    ipc.read_frames(reader, delivered.append)

    assert delivered == [ipc.Exited("whole")]
