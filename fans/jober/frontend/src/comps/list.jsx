import { useState, useEffect, useCallback } from 'react'
import { List as AntdList } from 'antd';

import { useQuery, setQuery } from 'src/utils';

export default function List({
  items=[],
  render=(() => null),
  domain=undefined,
  actions=[],
  style={},
  onSelected=(() => null),
  getItemId=(d => d.id),
}) {
  const [currentItem, _set_currentItem] = useState(null);
  const set_currentItem = useCallback(item => {
    _set_currentItem(item);
    onSelected(item);
    setQuery({[domain]: item ? getItemId(item) : undefined});
  }, []);
  const query = useQuery();

  useEffect(() => {
    if (items.length && !currentItem) {
      const item = items.filter(item => getItemId(item) == query[domain])[0];
      if (item) {
        set_currentItem(item);
      }
    }
  }, [items, query[domain]]);

  return (
    <AntdList
      style={style}
      bordered
      size="small"
      dataSource={items}
      renderItem={item => (
        <AntdList.Item
          className="alice-hover"
          style={{background: (currentItem && getItemId(currentItem) == getItemId(item)) ? '#E7F3FD' : null}}
          actions={actions}
          onClick={() => {
            if (currentItem && getItemId(currentItem) == getItemId(item)) {
              set_currentItem(null);
            } else {
              set_currentItem(item);
            }
          }}
        >
          {render(item)}
        </AntdList.Item>
      )}
    />
  );
}
