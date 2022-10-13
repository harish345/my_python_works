def parse_csv(csv: str, separator: str=',', quote: str='"') -> list:
    if csv=="":
      return[[""]]
    op = []
    lines = csv.splitlines()
    for line in lines:
        cols = line.split(separator)
        cols = [col for col in cols if col]
        data = [col.replace(quote,"") for col in cols]
        dlist=[d.splitlines() if d.splitlines() else d for d in data]
        lt = []
        for d in dlist:
            if d:
              if d[0]:
                lt.append(d[0])
        op.append(lt)
    return op


csv = """1,2,3
4,'this ''is''
a test',6"""

print(parse_csv(csv,',',"'"))