import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

export let history = null;
export let navigate = null;

export let uploadRef = null;
export const uploadContext = {
  onChange: () => {},
};
export let uploadDirRef = null;
export const uploadDirContext = {
  onChange: () => {},
};

export function useInitializeGlobals({
  uploadRef: _uploadRef,
  uploadDirRef: _uploadDirRef,
} = {}) {
  // TODO: useNavigate api incompatible with useHistory
  const _history = useNavigate();
  useEffect(() => {
    navigate = history = _history;
  }, [_history]);
}
