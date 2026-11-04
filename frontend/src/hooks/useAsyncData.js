import { useState, useEffect, useRef } from 'react';

/**
 * useAsyncData — key가 바뀔 때마다 load()를 다시 호출하는 조회 훅.
 *
 * 로딩 여부를 effect 안에서 setState로 켜지 않고 "마지막으로 받은 응답의 key가
 * 지금 key와 같은가"로 계산한다. 그래서 key가 바뀌는 즉시 이전 데이터는 숨겨지고,
 * 늦게 도착한 이전 요청의 응답은 버려진다.
 *
 * key가 null이면 조회하지 않는다.
 * Returns: { data, loading }
 */
export default function useAsyncData(key, load) {
    const loadRef = useRef(load);
    const [result, setResult] = useState({ key: null, data: null });

    useEffect(() => {
        loadRef.current = load;
    });

    useEffect(() => {
        if (key == null) return;
        let cancelled = false;
        loadRef.current()
            .then(data => { if (!cancelled) setResult({ key, data }); })
            .catch(() => { if (!cancelled) setResult({ key, data: null }); });
        return () => { cancelled = true; };
    }, [key]);

    const loading = key != null && result.key !== key;
    return { data: loading ? null : result.data, loading };
}
