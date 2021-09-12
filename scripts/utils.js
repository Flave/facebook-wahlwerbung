export const reduceBy = (arr, key = 'id') => {
  return arr.reduce((acc, d) => {
    acc[d[key]] = d;
    return acc;
  }, {});
};

export const groupBy = (arr, by = 'id') => {
  return arr.reduce((acc, d) => {
    const existing = acc[d[by]];
    if (existing) existing.push(d);
    else acc[d[by]] = [d];
    return acc;
  }, {});
};
