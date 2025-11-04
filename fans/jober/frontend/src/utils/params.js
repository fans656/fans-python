import React, { useMemo, useState, useEffect, useCallback } from 'react';
import _ from 'lodash';
import qs from 'qs';
import { useLocation } from 'react-router-dom';

import { navigate } from 'src/globals';

export function useBoundParam(name, {
  to: toParam = _.identity,
  from: fromParam = _.identity,
  when = false,
  init: getDefaultParam = _.identity,
}) {
  const query = useQuery();
  const [current, setCurrent] = useState(fromParam(query[name]));
  const setCurrentAndParam = useCallback(cur => {
    setCurrent(cur);
    if (cur) {
      setQuery({[name]: toParam(cur)});
    }
  }, []);
  useEffect(() => {
    if (when && getDefaultParam) {
      setCurrentAndParam(fromParam(getDefaultParam(query[name])));
    }
  }, [when]);
  return [current, setCurrentAndParam];
}

export function useQuery() {
  const location = useLocation();
  return React.useMemo(() => parseQuery(location), [location]);
}

export function getQuery() {
  return parseQuery((window || {}).location);
}

export function parseQuery(location) {
  return _parseQuery(location ? location.search.substring(1) : '');
}

export function setQuery(fields, { clear = false, replace = false } = {}) {
  if (_.isFunction(fields)) {
    fields = fields(getQuery());
  }

  if (!fields || !navigate) return;

  let params = {};
  if (!clear) {
    params = _parseQuery(window.location.search.substring(1));
  }

  for (const [key, value] of Object.entries(fields)) {
    if (params[key] !== value) {
      if (value == null) {
        delete params[key];
      } else {
        params[key] = value;
      }
    }
  }

  const search = qs.stringify(params, _qs_options);

  let url = window.location.pathname;
  if (search) {
    url += '?' + search;
  }

  navigate(url, {replace: replace});
}

export function setPath(path) {
  navigate(path, {replace: true});
}

function _parseQuery(queryText) {
  return qs.parse(queryText, _qs_options);
}

const _qs_options = {
  encode: false,
  arrayFormat: 'comma',
  commaRoundTrip: true,
  allowEmptyArrays: true,
  decoder: (str, defaultDecoder, charset, type) => {
    if (type === 'value') {
      //if (/^(\d+|\d+\.\d+)$/.test(str)) {
      //  const value = parseFloat(str);
      //  if (str.startsWith('0') && value !== 0) {
      //    return str;
      //  } else {
      //    return value;
      //  }
      //}
      if (str === 'true') return true;
      if (str === 'false') return false;
    }
    return defaultDecoder(str, defaultDecoder, charset);
  },
};
