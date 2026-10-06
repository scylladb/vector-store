# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""The `vsbench exec`/`ssh` guard: commands refused without --i-mean-it.

Two kinds of commands are refused:
- power-off commands: an instance terminates when it shuts down (a plain reboot is safe);
- commands that disarm the on-node TTL: vsbench-ttl.timer and its files are the only thing
  that terminates a forgotten cluster, and `vsbench extend` is the way to keep one alive.

Power-off verbs count only where a command word can appear: at the start, after ; & | ( { !
` $( or then/do/else, behind wrappers such as sudo/env/nohup/timeout (with their flags) and
VAR=value words, or inside a `sh -c`/eval script. Quoted arguments such as grep patterns are
blanked out first, so `journalctl ... | grep -iE "error|shutdown"` may run. remote.py
re-exports is_dangerous, is_ttl_tamper and refusal_reason.
"""

from __future__ import annotations

import re

_CMD_START = r"(?:^|[;&|({!`\n]|\$\(|\b(?:then|do|else)\s)\s*"
_WRAPPER_WORDS = r"sudo|doas|env|exec|nohup|command|nice|ionice|time|timeout|setsid|stdbuf|xargs|systemd-run"
# flags (an argument of -u/-g/... never starts with - or a digit, so every token parses one way only:
# overlapping alternatives would make the regex backtrack exponentially), and numbers (timeout 10)
_WRAPPER_ARGS = r"(?:\s+(?:-[ugsCDhpU]\s+[^\s;&|\d-][^\s;&|]*|-\S+|\d\S*))*"
_WRAPPERS = rf"(?:(?:[A-Za-z_]\w*=\S*|(?:\S*/)?(?:{_WRAPPER_WORDS}){_WRAPPER_ARGS})\s+)*"
_POWER_OFF_VERBS = (
    r"(?:\S*/)?(?:shutdown|poweroff|halt|kexec|(?:tel)?init\s+0"
    r"|reboot(?:\s+-\S+)*\s+(?:-[a-z]*p[a-z]*|--poweroff)|systemctl(?:\s+-\S+)*\s+(?:poweroff|halt|kexec))\b"
)
_POWER_OFF_TARGET = (
    r"\bsystemctl(?:\s+-\S+)*\s+(?:start|isolate|restart|try-restart|reload-or-restart)\b"
    r"[^;&|\n]*\b(?:runlevel0|poweroff|halt|kexec)(?:\.target)?\b"
)
_DANGEROUS_RE = re.compile(f"{_CMD_START}{_WRAPPERS}{_POWER_OFF_VERBS}|{_POWER_OFF_TARGET}|sysrq-trigger")

_WORD = r"(?<![\w.-])"  # the start of a whole word
_TTL_FILES = r"(?:/etc/vsbench/expires_at|ttl\.lastgood|/(?:usr/local/sbin|etc/systemd/system)/vsbench-ttl)"
_TTL_TAMPER_RE = re.compile(
    rf"{_WORD}systemctl(?:\s+-\S+)*\s+(?:stop|disable|mask|kill|reset-failed|edit|revert)\b[^;&|\n]*vsbench-ttl"
    rf"|(?:>|\bof=|{_WORD}(?:tee|mv|rm|unlink|shred|truncate|chmod|chown|chattr)\b"
    rf"|{_WORD}sed\b[^;&|\n]*\s(?:-[a-zE]*i|--in-place))[^;&|\n]*{_TTL_FILES}"
    rf"|{_WORD}(?:cp|install|ln|rsync)\b[^;&|\n]*\s\S*{_TTL_FILES}\S*\s*(?:$|[;&|\n)])"
)
_QUOTED_RE = re.compile(r"""(\b(?:ba|da|z)?sh\s+(?:-\S+\s+)*-[a-z]*c\s+|\beval\s+)?(['"])(.*?)\2""", re.S)
_BARE_WORD_RE = re.compile(r"[\w./-]*")
MAX_QUOTE_DEPTH = 5

POWER_OFF_REASON = (
    "it powers the node off, which terminates the instance and loses its data (`reboot` is safe); "
    "if the match is only a search pattern, quote it or rephrase it (grep -i 'shut.down'), "
    "or grep the output of `vsbench logs NODE ...` locally"
)
TTL_TAMPER_REASON = (
    "it disarms the node's TTL (vsbench-ttl.timer, /etc/vsbench/expires_at, ttl.lastgood), which leaves the "
    "ExpiresAt tag and the local state stale and can leak the instances; use `vsbench extend --ttl DURATION`"
)


def _unquote_match(match: re.Match[str]) -> str:
    prefix, quote, body = match.group(1) or "", match.group(2), match.group(3)
    if prefix or (quote == '"' and ("$(" in body or "`" in body)):
        return f"{prefix}({body})"  # a script (or command substitution): checked as commands
    return body if _BARE_WORD_RE.fullmatch(body) else "_"


def _unquote(cmd: str) -> str:
    """The command as the guard sees it: a `sh -c`/eval script becomes `(script)`, a quoted bare
    word loses its quotes, and any other quoted argument (pattern, message) becomes `_`."""
    for _ in range(MAX_QUOTE_DEPTH):  # quotes nested inside a `sh -c` script
        cmd, before = _QUOTED_RE.sub(_unquote_match, cmd), cmd
        if cmd == before:
            break
    return cmd


def is_ttl_tamper(cmd: str) -> bool:
    """True for commands that stop/disable/mask vsbench-ttl or rewrite its files."""
    return bool(_TTL_TAMPER_RE.search(_unquote(cmd)))


def refusal_reason(cmd: str) -> str | None:
    """Why `exec`/`ssh` must not run `cmd` without --i-mean-it; None when it may run."""
    if _DANGEROUS_RE.search(_unquote(cmd)):
        return POWER_OFF_REASON
    return TTL_TAMPER_REASON if is_ttl_tamper(cmd) else None


def is_dangerous(cmd: str) -> bool:
    """True for commands that power a node off (terminating the instance) or disarm its TTL."""
    return refusal_reason(cmd) is not None
