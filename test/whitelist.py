from scocto.whitelist import StreamWhitelist

def test_from_text():
    whitelist = StreamWhitelist.FromText("C C1 CX\n# comment\nGT.LPAZ\n")
    assert whitelist == ['C.*.*.*', 'C1.*.*.*', 'CX.*.*.*', 'GT.LPAZ.*.*']

def test_from_file():
    filename = "/tmp/whitelist-test.txt"
    with open(filename, "w") as f:
        f.write("C C1 CX\n\n\n# comment\nGT.LPAZ\n\n")

    whitelist = StreamWhitelist.FromFile(filename)
    assert whitelist == ['C.*.*.*', 'C1.*.*.*', 'CX.*.*.*', 'GT.LPAZ.*.*']

if __name__ == "__main__":
    test_from_text()
    test_from_file()
