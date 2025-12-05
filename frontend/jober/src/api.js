import React from 'react';
import _ from 'lodash';
import qs from 'qs';
import { message } from 'antd';
import Cookies from 'js-cookie';

export function useState(spec, deps = []) {
  spec = normalizedSpec(spec);

  const [state, set_state] = React.useState(spec.default || null);
  
  React.useEffect(() => {
    (async () => {
      let res = await get(spec.path);
      if (spec.convert) {
        res = spec.convert(res);
      }
      set_state(res);
    })();
  }, [...deps]);

  return state;
  
  function normalizedSpec(spec) {
    if (_.isString(spec)) {
      return {
        path: spec,
      };
    } else {
      return spec;
    }
  }
}

export async function get(path, args, conf) {
  return await request('GET', path, args, null, conf);
}

export async function localGet(path, args, conf) {
  return await localRequest('GET', path, args, null, conf);
}

export async function post(path, data, args, conf) {
  return await request('POST', path, args, data, conf);
}

export async function localPost(path, data, args, conf) {
  return await localRequest('POST', path, args, data, conf);
}

export function useGet(path, args, conf) {
  if (_.isArray(conf)) {
    conf = {
      deps: conf,
    };
  }
  return useRequest('GET', path, args, null, conf);
}

export function usePost(path, data, args, raw) {
  return useRequest('POST', path, args, data);
}

export function useRequest(method, path, args, data, { raw, binary, deps } = {}) {
  const [result, setResult] = React.useState(null);
  
  const depsArr = _.isArray(deps) ? deps : [method, path, args, data, raw, binary];
  React.useEffect(() => {
    let isMounted = true;
    setResult(null);
    (async () => {
      const result = await request(method, path, args, data, {
        raw, binary,
      });
      if (isMounted) {
        setResult(result);
      }
    })();
    return () => {isMounted = false};
  }, [...depsArr]);
  return result;
}

export async function localRequest(method, path, args, data, conf = {}) {
  return await request(method, 'http://localhost:6560' + path, args, data, conf);
}

export async function request(
  method, path, args, data,
  { raw, binary, blob, throwError, suppressError, credentials } = {}
) {
  return new Promise(async (resolve, reject) => {
    const originalPath = path;
    const headers = {};
    if (args) {
      path += '?' + qs.stringify(args);
    }
    if (method === 'POST') {
      headers['Content-Type'] = 'application/json';
      if (!data) {
        data = {};
      }
    }
    if (path.startsWith('http') && new URL(path).hostname.endsWith('.fans656.me')) {
      headers['fme-token'] = Cookies.get('token');
    }
    const res = await fetch(path, {
      method: method,
      headers: headers,
      body: data && JSON.stringify(data),
      credentials: credentials,
    });
    if (res.status === 200) {
      let result;
      if (blob) {
        result = await res.blob();
      } else if (binary) {
        result = await res.arrayBuffer();
      } else if (raw) {
        result = await res.text();
      } else {
        try {
          result = await res.json();
        } catch (e) {
          console.log('api.request.parse.error', await res.text());
          throw e;
        }
      }
      resolve(result);
      console.debug('api.request', {method, originalPath, args, data, res: result});
    } else {
      if (!raw) {
        const error = await res.json();
        if (!suppressError) {
          message.error(res.status + ': ' + error.reason);
        }
        if (res.status === 500) {
          console.log(error.traceback);
        }
        console.log('api.request.error', {method, path: originalPath, args, data}, 'error', error);
        if (throwError) {
          reject(error);
        }
      } else {
        console.log('api.request.error', res);
        reject(res);
      }
    }
  });
}

