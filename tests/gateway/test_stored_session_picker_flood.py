

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_stored_picker_drops_an_empty_flood_before_the_limit(tmp_path, monkeypatch):
    """Rows with nothing to show must not spend the page: the newest rows here are empty, and a
    queued admission is not emptiness even when no message was written yet."""
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from gateway.session_controls import AuthorityConnection

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(run, '_load_gateway_config', lambda: {'platform_toolsets': {'cli': []}})
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    db = store._db
    for sid in ('real-new', 'real-old'):
        db.create_session(sid, source='cli', model='m', cwd=str(tmp_path))
        db.append_message(sid, 'user', sid + ' question')
        db.append_message(sid, 'assistant', sid + ' answer')
        db.end_session(sid, 'cli_exit')
    db.create_session('queued-empty', source='cli', model='m', cwd=str(tmp_path))
    db.end_session('queued-empty', 'cli_exit')
    with db._lock:
        db._conn.execute(
            "INSERT INTO session_admissions(admission_id,request_id,principal_id,target_session_id,"
            "lineage_json,payload_json,payload_digest,intent,status) "
            "VALUES('adm','req','native','queued-empty','[]','{}','d','queue','queued')")
        db._conn.commit()
    for i in range(40):  # newer than the real history, and provably empty
        sid = f'empty-{i}'
        db.create_session(sid, source='cli', model='m', cwd=str(tmp_path))
        db.end_session(sid, 'cli_exit')
        with db._lock:
            db._conn.execute('UPDATE sessions SET started_at = started_at + 3600 WHERE id = ?', (sid,))
            db._conn.commit()
    runner = SimpleNamespace(session_store=store, _session_db=db, adapters={}, _draining=False,
                             _cached_agent_for=lambda route: None)
    runner._adapter_for_source = lambda source: runner.adapters.get(source.platform)
    authority = await initialize_session_authority(runner, profile_id=str(tmp_path), instance_id='owner')
    native = AuthorityConnection(authority, object(), {'provider': 'local', 'user_id': 'native',
        'profile_id': str(tmp_path), 'instance_id': 'owner'})

    before = db._conn.total_changes
    result = await native.dispatch({'id': 1, 'method': 'session.list',
                                    'params': {'scope': 'stored', 'limit': 3}})
    assert 'result' in result, result
    rows = result['result']['sessions']
    assert [r['id'] for r in rows] and {r['id'] for r in rows} == {'real-new', 'real-old', 'queued-empty'}
    assert [r['activity_at'] for r in rows] == sorted((r['activity_at'] for r in rows), reverse=True)
    # Enumeration still adopts nothing.
    assert db._conn.total_changes == before
    assert authority.sessions == {}

    wider = await native.dispatch({'id': 2, 'method': 'session.list',
                                   'params': {'scope': 'stored', 'limit': 200, 'picker': True}})
    assert {r['id'] for r in wider['result']['sessions']} == {'real-new', 'real-old', 'queued-empty'}, wider
