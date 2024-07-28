from dataclasses import dataclass
from typing import List
import os
import xml.etree.ElementTree as ET
import sys


@dataclass
class ScriptCoverage:
    filename: str
    cover: float


def get_coverage(coverage_path: str) -> List[ScriptCoverage]:
    tree = ET.parse(coverage_path)
    root = tree.getroot()

    files = []

    for packages in root.findall("packages"):
        for package in packages.findall("package"):
            for classes in package.findall("classes"):
                for coverage in classes.findall("class"):
                    files.append(
                        ScriptCoverage(
                            filename=coverage.attrib.get("filename"),
                            cover=float(coverage.attrib.get("line-rate")),
                        )
                    )
    return files


def get_invalid_files(total_scripts: List[ScriptCoverage], min_coverage: float) -> List[ScriptCoverage]:
    invalid_files = []
    for coverage in total_scripts:
        if coverage.cover < min_coverage:
            invalid_files.append(coverage)
    return invalid_files


def print_message_error(invalid_script: List[ScriptCoverage]) -> None:
    for script in invalid_script:
        print(f"For file {script.filename} the tests only cover {script.cover * 100}%")


if __name__ == "__main__":
    coverage_path = os.path.join(os.getcwd(), "coverage.xml")
    min_coverage = float(os.environ.get("MIN_COVERAGE", 0.8))

    coverage = get_coverage(coverage_path)

    if invalid_scripts := get_invalid_files(coverage, min_coverage):
        print(f"All files that do not reach {min_coverage * 100}% code coverage are listed below.".center(100, "*"))
        print_message_error(invalid_scripts)
        sys.exit(1)
