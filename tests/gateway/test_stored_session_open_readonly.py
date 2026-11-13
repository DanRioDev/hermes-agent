"""Persisted local history is readable without claiming another runtime's writer."""
import os
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.gateway.test_session_mutation_model_busy import _owner


def snapshot(db):
    tables = ('sessions', 'messages', 'state_meta', 'runtime_epoch', 'gateway_routing',
              'session_admissions', 'worker_executions', 'worker_receipts', 'session_turn_leases')
    with db._read_ctx() as conn:
        return {t: [tuple(r) for r in conn.execute(f'SELECT * FROM {t} ORDER BY rowid')] for t in tables}


@pytest.mark.asyncio
async def test_other_runtime_transcript_and_title_never_adopt(tmp_path, monkeypatch):
    from gateway.session_authority import SessionAuthority
    from gateway.session_controls import AuthorityConnection
    from gateway.session_history_mount import is_history_mount
    from hermes_state_runtime import register_worker_execution, apply_worker_receipt
    async def handle(event):
        return 'ok'
    store, db, authority, owner, ref = _owner(tmp_path, monkeypatch, handle)
    peers = []
    try:
        db.append_message(ref.session_id, 'user', 'first\n\nsecond')
        db.set_session_title(ref.session_id, 'Readonly title')
        generation = db.get_session(ref.session_id)['runtime_generation']
        register_worker_execution(db, epoch=authority.epoch, execution_id='fixture-worker',
            session_id=ref.session_id, generation=generation, kind='compute', adoption_secret='fixture-secret')
        apply_worker_receipt(db, epoch=authority.epoch, execution_id='fixture-worker',
            session_id=ref.session_id, generation=generation, sequence=1, operation='fixture.read',
            payload={}, handlers={'fixture.read': lambda *args: {'fixture': True}})
        assert db.try_acquire_session_turn_lease(ref.session_id, 'fixture-writer')
        runner = SimpleNamespace(session_store=store, _session_db=db, adapters={}, _draining=False,
                                 _cached_agent_for=lambda route: None)
        other = SessionAuthority(runner, profile_id=authority.profile_id, instance_id='reader',
                                 db=db, epoch=authority.epoch)
        peer = AuthorityConnection(other, object(), {'provider': 'local', 'user_id': owner.actor.subject,
            'profile_id': other.profile_id, 'instance_id': other.instance_id,
            'capabilities': ['session:read', 'session:create']})
        peers.append(peer)
        before, changes = snapshot(db), db._conn.total_changes
        entries = dict(store._entries)
        async def dispatch(method, params):
            reply = await peer.dispatch({'id': 1, 'method': method, 'params': params})
            assert 'result' in reply, reply
            return reply['result']
        titled = await dispatch('session.list', {'title': 'Readonly title'})
        assert titled['sessions'][0]['resumable'] is False
        assert other.sessions == {}
        listing = await dispatch('session.list', {'scope': 'stored', 'limit': 10})
        row = next(r for r in listing['sessions'] if r['id'] == ref.session_id)
        assert row['resumable'] is False and row['owner_runtime'] == 'other'
        transcript = await dispatch('session.transcript', {'session_id': ref.session_id, 'order': 'latest'})
        assert any('first\n\nsecond' in m.get('text', '') for m in transcript['rows'])
        assert is_history_mount(other.sessions[ref.session_id])
        assert peer.subscriptions == {} and runner.adapters == {} and store._entries == entries
        again = await dispatch('session.list', {'scope': 'stored'})
        assert any(r['id'] == ref.session_id and not r['resumable'] for r in again['sessions'])
        for kind in ('principal', 'profile'):
            denied = AuthorityConnection(other, object(), {'user_id': 'different' if kind == 'principal' else 'human'})
            peers.append(denied)
            if kind == 'profile':
                denied.actor = replace(denied.actor, profile_id='different')
            for method, params in [('session.transcript', {'session_id': ref.session_id}),
                                   ('session.list', {'title': 'Readonly title'})]:
                reply = await denied.dispatch({'id': 2, 'method': method, 'params': params})
                assert 'error' in reply or not reply['result']['sessions'], reply
        assert snapshot(db) == before and db._conn.total_changes == changes
    finally:
        for peer in peers:
            await peer.close()
        await owner.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_real_omh_adapter_app_readonly_dispatch(tmp_path, monkeypatch):
    root = os.environ.get('OMH_TEST_ROOT')
    if not root:
        sibling = Path(__file__).resolve().parents[2].parent / 'omh'
        if (sibling / 'src' / 'tui').is_dir():
            root = str(sibling)
    if not root:
        pytest.skip('Set OMH_TEST_ROOT to the hash-bound OMH mirror for cross-repository coverage')
    sys.path[:0] = [str(Path(root) / 'src'), str(Path(root) / 'tests')]
    from omh.tui.attach.gateway_attach import Adapter, TO_OWNER, TO_CLIENT
    from omh.tui.gateway import Response
    from omh.tui.keys import Key, ENTER
    from test_tui_modes import make_app
    from _tui_lines import line_text
    from gateway.session_authority import SessionAuthority
    from gateway.session_controls import AuthorityConnection
    from gateway.session_history_mount import is_history_mount
    async def handle(event):
        return 'ok'
    store, db, authority, owner, ref = _owner(tmp_path, monkeypatch, handle)
    peer = None
    try:
        db.append_message(ref.session_id, 'user', 'first\n\nsecond')
        db.set_session_title(ref.session_id, 'Readonly title')
        runner = SimpleNamespace(session_store=store, _session_db=db, adapters={}, _draining=False,
                                 _cached_agent_for=lambda route: None)
        other = SessionAuthority(runner, profile_id=authority.profile_id, instance_id='reader',
                                 db=db, epoch=authority.epoch)
        peer = AuthorityConnection(other, object(), {'provider': 'local', 'user_id': owner.actor.subject,
            'profile_id': other.profile_id, 'instance_id': other.instance_id,
            'capabilities': ['session:read', 'session:create']})
        adapter, trace = Adapter({}), []
        adapter.current = 'unchanged-send-target'
        async def pump(actions):
            pending, answers = list(actions), []
            while pending:
                target, frame = pending.pop(0)
                if target == TO_OWNER:
                    trace.append(frame)
                    pending.extend(adapter.from_owner(await peer.dispatch(frame)))
                else:
                    assert target == TO_CLIENT
                    answers.append(frame)
            return answers
        await pump(adapter.describe())
        before, changes = snapshot(db), db._conn.total_changes
        async def request(method, params):
            answers = await pump(adapter.from_client({'id': 7, 'method': method, 'params': params}))
            assert len(answers) == 1 and 'result' in answers[0], answers
            return answers[0]['result']
        listing = await request('session.list', {'limit': 5})
        assert len(listing['sessions']) == 1
        app, client = make_app()
        target = (app.sid, app.stored_id, adapter.current)
        app._open_session_picker(listing)
        app._picker_key(Key(ENTER))
        method, params = client.calls[-1]
        assert method == 'session.transcript'
        result = await request(method, params)
        app._on_response(Response(len(client.calls), result, None))
        text = '\n'.join(line_text(line) for line in app.pager.lines)
        assert 'first\n\nsecond' in text and 'Read-only' in app.pager.title
        assert target == (app.sid, app.stored_id, adapter.current)
        titled = await request('session.list', {'title': 'Readonly title'})
        app._resume_looked_up('Readonly title', {}, titled, '', app.switch_seq)
        assert client.calls[-1][0] == 'session.transcript'
        assert 'session.resume' not in [f['method'] for f in trace]
        assert is_history_mount(other.sessions[ref.session_id]) and peer.subscriptions == {}
        assert runner.adapters == {} and snapshot(db) == before and db._conn.total_changes == changes
    finally:
        if peer:
            await peer.close()
        await owner.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_live_empty_flood_keeps_watched_queued_running_and_history(tmp_path, monkeypatch):
    from gateway.session_local import create_local_session
    from hermes_state_runtime import admit_session_input, claim_session_input
    async def handle(event):
        return 'ok'
    store, db, authority, owner, ref = _owner(tmp_path, monkeypatch, handle)
    try:
        db.append_message(ref.session_id, 'user', 'real history')
        db._write_sql('UPDATE sessions SET message_count=0 WHERE id=?', (ref.session_id,))
        keep = {ref.session_id}
        for i in range(35):
            sid = create_local_session(authority, owner.actor, dict(request_id=f'flood-{i}', source='gui',
                cwd=str(tmp_path), model='frozen', toolsets=[])).session_id
            if i == 0:
                authority.sessions[sid].subscribers[owner.actor.transport_id] = object()
                keep.add(sid)
            elif i in (1, 2):
                admit_session_input(db, epoch=authority.epoch, principal_id=owner.actor.subject,
                    session_id=sid, request_id=f'q-{i}', payload={'text': 'queued'})
                if i == 2:
                    claim_session_input(db, epoch=authority.epoch, session_id=sid)
                keep.add(sid)
        before, changes = snapshot(db), db._conn.total_changes
        reply = await owner.dispatch({'id': 1, 'method': 'session.list', 'params': {'limit': 5}})
        assert 'result' in reply, reply
        assert {r['id'] for r in reply['result']['sessions']} == keep
        assert snapshot(db) == before and db._conn.total_changes == changes
    finally:
        await owner.close()
        store.close_all_db_handles()
