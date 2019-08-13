from collections import Counter

import plotly.graph_objects as go


def read_file(file_name):
    with open(file_name, "r") as file:
        file.readline()
        p = file.readline()
        _, _, node_num, arc_num = p.split(" ")
        file.readline()
        file.readline()
        node_num = int(node_num)
        arc_num = int(arc_num)
        label = [""] * 100
        source = [0] * arc_num
        target = [0] * arc_num
        value = [0] * arc_num
        comment = [0] * arc_num
        for _ in range(node_num):
            c = file.readline()
            n = file.readline()
            print(c, n)
            cc = c.split(" ")
            nn = n.split(" ")
            name = str(cc[2])
            idx = int(nn[1])
            label[idx - 1] = name
        x = file.readline()
        print(x)
        for i in range(arc_num):
            a = file.readline()
            try:
                _, start, end, _, capacity, cost = a.split(" ")
                source[i], target[i], value[i], comment[i] = int(start) - 1, int(end) - 1, int(capacity), int(cost)
            except:
                break
    colors = ["blue"] * len(label)
    ct = Counter(source)
    for idx, name in enumerate(label):
        if str(name).startswith("Res"):
            colors[idx] = "green"
        elif str(name).startswith("Task"):
            if ct[idx] == 1:
                colors[idx] = "red"
        elif str(name).startswith("UN"):
            colors[idx] = "black"
        elif str(name).startswith("SIN"):
            colors[idx] = "pink"

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=label,
            color=colors
        ),
        link=dict(
            source=source,
            # indices correspond to labels, eg A1, A2, A2, B1, ...
            target=target,
            value=value,
            label=comment
        ))])
    fig.update_layout(title_text="Basic Sankey Diagram", font_size=10)
    fig.show()


if __name__ == '__main__':
    read_file("mcmf_before")
    read_file("mcmf-after")
